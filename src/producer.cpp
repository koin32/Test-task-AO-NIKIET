// Producer - Производитель данных

#include <bits/stdc++.h>
#include <zlib.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <atomic>
#include <future>
#include <chrono>

using namespace std;

constexpr const char* SHARED_MEMORY_SEGMENT_NAME = "/shm_shr_channel_example";
constexpr size_t SHARED_MEMORY_SIZE = 256;
constexpr size_t MAX_PAYLOAD_CAPACITY = 200;

// Структура для обмена данными через shared memory
struct SharedMemoryHeader {
    uint32_t synchronization_flag;    // Spinlock для синхронизации
    uint32_t message_available;       // Флаг готовности сообщения
    uint32_t data_block_identifier;   // ID блока данных
    uint32_t fragment_sequence_number;// Порядковый номер фрагмента в блоке
    uint8_t  is_final_fragment;       // Флаг последнего фрагмента в блоке
    uint32_t actual_payload_length;   // Длина полезных данных
    uint8_t  data_payload[MAX_PAYLOAD_CAPACITY]; // Полезная нагрузка
} __attribute__((packed));

// Открывает и настраивает shared memory сегмент
static inline SharedMemoryHeader* initialize_shared_memory() {
    int shared_memory_descriptor = shm_open(SHARED_MEMORY_SEGMENT_NAME, O_CREAT | O_RDWR, 0600);
    if (shared_memory_descriptor < 0) { perror("shm_open"); exit(1); }
    if (ftruncate(shared_memory_descriptor, SHARED_MEMORY_SIZE) != 0) { perror("ftruncate"); exit(1); }
    void* memory_region = mmap(nullptr, SHARED_MEMORY_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, shared_memory_descriptor, 0);
    if (memory_region == MAP_FAILED) { perror("mmap"); exit(1); }
    close(shared_memory_descriptor);
    return reinterpret_cast<SharedMemoryHeader*>(memory_region);
}

// Блокирует shared memory с помощью spinlock
static inline void acquire_shared_memory_lock(SharedMemoryHeader* shared_mem) {
    uint32_t desired_state = 0;
    while (!__atomic_compare_exchange_n(&shared_mem->synchronization_flag, &desired_state, 1, false,
                                        __ATOMIC_ACQ_REL, __ATOMIC_RELAXED)) {
        desired_state = 0;
        this_thread::yield();
    }
}

// Разблокирует shared memory
static inline void release_shared_memory_lock(SharedMemoryHeader* shared_mem) {
    __atomic_store_n(&shared_mem->synchronization_flag, 0, __ATOMIC_RELEASE);
}

// Сжимает блок данных с помощью zlib
vector<uint8_t> deflate_data_block(const vector<uint8_t>& input_data, int compression_level = Z_BEST_SPEED) {
    if (input_data.empty()) {
        return {}; // Для пустых данных возвращаем пустой вектор
    }
    
    uLong source_data_length = input_data.size();
    uLong maximum_compressed_size = compressBound(source_data_length);
    vector<uint8_t> compressed_output(maximum_compressed_size);
    uLongf final_compressed_size = maximum_compressed_size;
    int compression_result = compress2(compressed_output.data(), &final_compressed_size, input_data.data(), source_data_length, compression_level);
    if (compression_result != Z_OK) {
        cerr << "Ошибка сжатия: " << compression_result << endl;
        return input_data; // При ошибке возвращаем исходные данные
    }
    compressed_output.resize(final_compressed_size);
    return compressed_output;
}

int main(int argc, char** argv){
    ios::sync_with_stdio(false);
    
    // Проверка аргументов командной строки
    if (argc < 2) {
        cerr << "Использование: producer <входной_файл>" << endl;
        cerr << "Пример: producer source_data.bin" << endl;
        return 1;
    }

    string input_filename = argv[1];
    
    // Проверка существования и доступности файла
    ifstream input_file_stream(input_filename, ios::binary);
    if (!input_file_stream) { 
        cerr << "Ошибка: Не удалось открыть файл '" << input_filename << "'" << endl;
        return 1; 
    }
    
    // Определяем размер файла
    input_file_stream.seekg(0, ios::end);
    size_t total_file_size = input_file_stream.tellg();
    input_file_stream.seekg(0, ios::beg);
    
    cout << "Входной файл: " << input_filename << endl;
    cout << "Размер файла: " << total_file_size << " байт" << endl;
    
    if (total_file_size == 0) {
        cout << "Предупреждение: Файл пустой" << endl;
    }

    // Читаем содержимое файла
    vector<uint8_t> file_content(total_file_size);
    if (total_file_size > 0) {
        input_file_stream.read(reinterpret_cast<char*>(file_content.data()), total_file_size);
        if (!input_file_stream) {
            cerr << "Ошибка чтения файла" << endl;
            return 1;
        }
    }
    input_file_stream.close();

    // Разбиваем файл на блоки по 64 КБ
    const size_t UNCOMPRESSED_BLOCK_SIZE = 64 * 1024;
    size_t total_blocks_count = (total_file_size + UNCOMPRESSED_BLOCK_SIZE - 1) / UNCOMPRESSED_BLOCK_SIZE;
    
    cout << "Размер блока: " << UNCOMPRESSED_BLOCK_SIZE << " байт" << endl;
    cout << "Количество блоков: " << total_blocks_count << endl;

    // Инициализируем shared memory
    SharedMemoryHeader* shared_memory_region = initialize_shared_memory();
    memset(shared_memory_region, 0, sizeof(SharedMemoryHeader));

    // Обработка пустого файла
    if (total_blocks_count == 0) {
        cout << "Отправка маркера пустого файла..." << endl;
        
        // Ждем освобождения канала
        while (true) {
            acquire_shared_memory_lock(shared_memory_region);
            if (shared_memory_region->message_available == 0) break;
            release_shared_memory_lock(shared_memory_region);
            this_thread::yield();
        }
        
        // Отправляем маркер пустого файла
        shared_memory_region->data_block_identifier = 0;
        shared_memory_region->fragment_sequence_number = 0;
        shared_memory_region->is_final_fragment = 1;
        shared_memory_region->actual_payload_length = 0;
        shared_memory_region->message_available = 1;
        release_shared_memory_lock(shared_memory_region);
        
        // Ждем подтверждения приема
        while (true) {
            acquire_shared_memory_lock(shared_memory_region);
            bool message_still_available = (shared_memory_region->message_available != 0);
            release_shared_memory_lock(shared_memory_region);
            if (!message_still_available) break;
            this_thread::yield();
        }
        
        cout << "Маркер пустого файла отправлен" << endl;
    } else {
        // Многопоточное сжатие блоков
        vector<future<vector<uint8_t>>> compression_tasks;
        compression_tasks.reserve(total_blocks_count);
        
        cout << "Запуск параллельного сжатия..." << endl;
        
        // Запускаем сжатие каждого блока в отдельном потоке
        for (size_t block_index = 0; block_index < total_blocks_count; ++block_index) {
            size_t data_offset = block_index * UNCOMPRESSED_BLOCK_SIZE;
            size_t block_length = min(UNCOMPRESSED_BLOCK_SIZE, total_file_size - data_offset);
            vector<uint8_t> data_block(file_content.begin() + data_offset, file_content.begin() + data_offset + block_length);
            
            compression_tasks.push_back(async(launch::async, [data_block]() {
                return deflate_data_block(data_block, Z_BEST_SPEED);
            }));
        }

        auto compression_start_time = chrono::steady_clock::now();
        size_t total_compressed_size = 0;

        // Передача сжатых данных через shared memory
        for (size_t current_block_id = 0; current_block_id < total_blocks_count; ++current_block_id) {
            // Получаем сжатый блок
            vector<uint8_t> compressed_block = compression_tasks[current_block_id].get();
            total_compressed_size += compressed_block.size();

            // Разбиваем на фрагменты и передаем
            size_t current_offset = 0;
            uint32_t fragment_counter = 0;
            
            while (current_offset < compressed_block.size()) {
                size_t fragment_size = min(MAX_PAYLOAD_CAPACITY, compressed_block.size() - current_offset);

                // Ожидаем освобождения канала
                while (true) {
                    acquire_shared_memory_lock(shared_memory_region);
                    if (shared_memory_region->message_available == 0) break;
                    release_shared_memory_lock(shared_memory_region);
                    this_thread::yield();
                }

                // Записываем данные в shared memory
                shared_memory_region->data_block_identifier = (uint32_t)current_block_id;
                shared_memory_region->fragment_sequence_number = fragment_counter;
                shared_memory_region->is_final_fragment = (current_offset + fragment_size >= compressed_block.size()) ? 1 : 0;
                shared_memory_region->actual_payload_length = static_cast<uint32_t>(fragment_size);
                memcpy(shared_memory_region->data_payload, compressed_block.data() + current_offset, fragment_size);
                shared_memory_region->message_available = 1;
                release_shared_memory_lock(shared_memory_region);

                // Ожидаем подтверждения приема
                while (true) {
                    acquire_shared_memory_lock(shared_memory_region);
                    bool message_still_available = (shared_memory_region->message_available != 0);
                    release_shared_memory_lock(shared_memory_region);
                    if (!message_still_available) break;
                    this_thread::yield();
                }

                current_offset += fragment_size;
                ++fragment_counter;
            }
            
            // Вывод прогресса
            if ((current_block_id + 1) % 10 == 0 || current_block_id + 1 == total_blocks_count) {
                cout << "Прогресс: " << (current_block_id + 1) << "/" << total_blocks_count << " блоков отправлено" << endl;
            }
        }

        auto compression_end_time = chrono::steady_clock::now();
        double total_compression_time = chrono::duration<double>(compression_end_time - compression_start_time).count();
        cout << "Все блоки данных успешно отправлены" << endl;
        cout << "Время выполнения: " << total_compression_time << " секунд" << endl;
        cout << "Статистика сжатия:" << endl;
        cout << "  Исходный размер: " << total_file_size << " байт" << endl;
        cout << "  Сжатый размер: " << total_compressed_size << " байт" << endl;
        if (total_file_size > 0) {
            double compression_ratio = 100.0 * (1.0 - double(total_compressed_size) / double(total_file_size));
            cout << "  Степень сжатия: " << fixed << setprecision(2) << compression_ratio << " %" << endl;
        }
    }

    // Отправка сигнала завершения
    cout << "Отправка сигнала завершения..." << endl;
    
    while (true) {
        acquire_shared_memory_lock(shared_memory_region);
        if (shared_memory_region->message_available == 0) break;
        release_shared_memory_lock(shared_memory_region);
        this_thread::yield();
    }
    
    shared_memory_region->data_block_identifier = UINT32_MAX;
    shared_memory_region->fragment_sequence_number = 0;
    shared_memory_region->is_final_fragment = 1;
    shared_memory_region->actual_payload_length = 0;
    shared_memory_region->message_available = 1;
    release_shared_memory_lock(shared_memory_region);

    cout << "Сигнал завершения отправлен" << endl;

    // Освобождение ресурсов
    munmap(reinterpret_cast<void*>(shared_memory_region), SHARED_MEMORY_SIZE);
    cout << "Работа producer завершена успешно" << endl;
    return 0;
}
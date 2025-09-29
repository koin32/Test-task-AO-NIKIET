// Consumer - Потребитель данных

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

// Структура shared memory (должна совпадать с producer)
struct SharedMemoryHeader {
    uint32_t synchronization_flag;
    uint32_t message_available;
    uint32_t data_block_identifier;
    uint32_t fragment_sequence_number;
    uint8_t  is_final_fragment;
    uint32_t actual_payload_length;
    uint8_t  data_payload[MAX_PAYLOAD_CAPACITY];
} __attribute__((packed));

// Функции работы с shared memory (аналогичны producer)
static inline SharedMemoryHeader* initialize_shared_memory() {
    int shared_memory_descriptor = shm_open(SHARED_MEMORY_SEGMENT_NAME, O_CREAT | O_RDWR, 0600);
    if (shared_memory_descriptor < 0) { perror("shm_open"); exit(1); }
    if (ftruncate(shared_memory_descriptor, SHARED_MEMORY_SIZE) != 0) { perror("ftruncate"); exit(1); }
    void* memory_region = mmap(nullptr, SHARED_MEMORY_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, shared_memory_descriptor, 0);
    if (memory_region == MAP_FAILED) { perror("mmap"); exit(1); }
    close(shared_memory_descriptor);
    return reinterpret_cast<SharedMemoryHeader*>(memory_region);
}

static inline void acquire_shared_memory_lock(SharedMemoryHeader* shared_mem) {
    uint32_t desired_state = 0;
    while (!__atomic_compare_exchange_n(&shared_mem->synchronization_flag, &desired_state, 1, false,
                                        __ATOMIC_ACQ_REL, __ATOMIC_RELAXED)) {
        desired_state = 0;
        this_thread::yield();
    }
}

static inline void release_shared_memory_lock(SharedMemoryHeader* shared_mem) {
    __atomic_store_n(&shared_mem->synchronization_flag, 0, __ATOMIC_RELEASE);
}

// Распаковывает сжатые данные с помощью zlib
vector<uint8_t> inflate_compressed_data(const vector<uint8_t>& compressed_input) {
    if (compressed_input.empty()) {
        return {}; // Пустые входные данные
    }
    
    // Выделяем буфер с запасом
    size_t output_buffer_capacity = compressed_input.size() * 6 + 1024;
    vector<uint8_t> decompressed_output(output_buffer_capacity);
    uLongf final_decompressed_size = output_buffer_capacity;

    // Первая попытка распаковки
    int decompression_result = uncompress(decompressed_output.data(), &final_decompressed_size, compressed_input.data(), compressed_input.size());
    
    // Если буфера не хватило - увеличиваем и пробуем снова
    if (decompression_result == Z_BUF_ERROR) {
        output_buffer_capacity *= 2;
        decompressed_output.resize(output_buffer_capacity);
        final_decompressed_size = output_buffer_capacity;
        decompression_result = uncompress(decompressed_output.data(), &final_decompressed_size, compressed_input.data(), compressed_input.size());
    }

    if (decompression_result != Z_OK) {
        cerr << "Ошибка распаковки: " << decompression_result << endl;
        return {};
    }

    decompressed_output.resize(final_decompressed_size);
    return decompressed_output;
}

int main(int argc, char** argv) {
    // Проверка аргументов командной строки
    if (argc < 2) {
        cerr << "Использование: consumer <выходной_файл>" << endl;
        cerr << "Пример: consumer output.bin" << endl;
        return 1;
    }
    
    string output_filename = argv[1];
    cout << "Выходной файл: " << output_filename << endl;
    
    // Открываем файл для записи
    FILE* output_file_stream = fopen(output_filename.c_str(), "wb");
    if (!output_file_stream) { 
        perror("Ошибка открытия файла");
        return 1; 
    }

    // Инициализируем shared memory
    SharedMemoryHeader* shared_memory_region = initialize_shared_memory();

    // Структуры для управления данными
    uint32_t next_expected_block_id = 0; // Ожидаемый номер блока
    unordered_map<uint32_t, vector<uint8_t>> block_assembly_buffer; // Сборка фрагментов
    map<uint32_t, future<vector<uint8_t>>> decompression_tasks; // Задачи распаковки

    auto program_start_time = chrono::steady_clock::now();
    bool termination_signal_received = false; // Флаг завершения работы
    bool empty_file_detected = false; // Флаг пустого файла

    cout << "Consumer запущен. Ожидание данных..." << endl;

    // Главный цикл обработки данных
    while (!termination_signal_received) {
        // Ожидаем новых данных от producer
        while (true) {
            acquire_shared_memory_lock(shared_memory_region);
            if (shared_memory_region->message_available == 1) break;
            release_shared_memory_lock(shared_memory_region);
            this_thread::sleep_for(chrono::milliseconds(1));
        }

        // Читаем данные из shared memory
        uint32_t current_block_id = shared_memory_region->data_block_identifier;
        uint8_t is_last_fragment = shared_memory_region->is_final_fragment;
        uint32_t payload_size = shared_memory_region->actual_payload_length;
        
        vector<uint8_t> received_payload(payload_size);
        if (payload_size > 0) {
            memcpy(received_payload.data(), shared_memory_region->data_payload, payload_size);
        }

        // Освобождаем shared memory для следующего сообщения
        shared_memory_region->message_available = 0;
        release_shared_memory_lock(shared_memory_region);

        cout << "Получено: block_id=" << current_block_id << ", last_chunk=" << (int)is_last_fragment 
             << ", размер данных=" << payload_size << " байт" << endl;

        // Проверяем сигнал завершения
        if (current_block_id == UINT32_MAX) {
            termination_signal_received = true;
            cout << "Получен сигнал завершения" << endl;
            break;
        }

        // Проверяем маркер пустого файла
        if (current_block_id == 0 && is_last_fragment == 1 && payload_size == 0) {
            cout << "Получен маркер пустого файла" << endl;
            empty_file_detected = true;
            block_assembly_buffer[current_block_id] = {}; // Создаем пустой блок
        } else {
            // Добавляем фрагмент к соответствующему блоку
            auto &assembly_buffer = block_assembly_buffer[current_block_id];
            assembly_buffer.insert(assembly_buffer.end(), received_payload.begin(), received_payload.end());
        }

        // Если блок собран полностью
        if (is_last_fragment) {
            auto compressed_data = block_assembly_buffer[current_block_id];
            cout << "Блок " << current_block_id << " собран, размер: " << compressed_data.size() << " байт" << endl;
            
            // Запускаем распаковку в отдельном потоке
            if (compressed_data.empty()) {
                // Для пустого блока создаем пустой результат
                decompression_tasks[current_block_id] = async(launch::async, []() {
                    return vector<uint8_t>{};
                });
            } else {
                // Запускаем распаковку сжатых данных
                decompression_tasks[current_block_id] = async(launch::async, [compressed_data]() {
                    return inflate_compressed_data(compressed_data);
                });
            }
            block_assembly_buffer.erase(current_block_id);
        }

        // Записываем готовые блоки в правильном порядке
        while (true) {
            auto task_iterator = decompression_tasks.find(next_expected_block_id);
            if (task_iterator == decompression_tasks.end()) break;
            
            // Проверяем, завершилась ли распаковка
            if (task_iterator->second.wait_for(chrono::milliseconds(0)) != future_status::ready) {
                break;
            }
            
            // Получаем распакованные данные
            vector<uint8_t> decompressed_data = task_iterator->second.get();
            
            // Записываем в файл
            if (!decompressed_data.empty() || empty_file_detected) {
                size_t bytes_written = fwrite(decompressed_data.data(), 1, decompressed_data.size(), output_file_stream);
                fflush(output_file_stream);
                cout << "Блок " << next_expected_block_id << " записан в файл: " 
                     << bytes_written << " байт" << endl;
            } else {
                cout << "Блок " << next_expected_block_id << " пропущен (пустой или ошибка распаковки)" << endl;
            }
            
            // Переходим к следующему блоку
            decompression_tasks.erase(task_iterator);
            ++next_expected_block_id;
        }
    }

    // Обрабатываем оставшиеся блоки
    cout << "Обработка оставшихся блоков: " << decompression_tasks.size() << endl;
    
    for (auto &decompression_task : decompression_tasks) {
        vector<uint8_t> decompressed_data = decompression_task.second.get();
        if (!decompressed_data.empty()) {
            size_t bytes_written = fwrite(decompressed_data.data(), 1, decompressed_data.size(), output_file_stream);
            cout << "Поздний блок " << decompression_task.first << " записан: " << bytes_written << " байт" << endl;
        } else {
            cout << "Поздний блок " << decompression_task.first << " пропущен (пустой)" << endl;
        }
    }

    // Выводим статистику
    auto program_end_time = chrono::steady_clock::now();
    double total_execution_time = chrono::duration<double>(program_end_time - program_start_time).count();
    
    cout << "Работа consumer завершена успешно" << endl;
    cout << "Общее время: " << total_execution_time << " секунд" << endl;
    cout << "Обработано блоков: " << next_expected_block_id << endl;
    
    // Определяем размер выходного файла
    fseek(output_file_stream, 0, SEEK_END);
    long final_output_size = ftell(output_file_stream);
    cout << "Размер выходного файла: " << final_output_size << " байт" << endl;

    // Освобождаем ресурсы
    fclose(output_file_stream);
    munmap(reinterpret_cast<void*>(shared_memory_region), SHARED_MEMORY_SIZE);
    shm_unlink(SHARED_MEMORY_SEGMENT_NAME);
    
    cout << "Ресурсы освобождены" << endl;
    return 0;
}
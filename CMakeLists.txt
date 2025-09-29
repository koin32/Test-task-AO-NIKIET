# Минимальная требуемая версия CMake
cmake_minimum_required(VERSION 3.10)

# Имя проекта и язык
project(shm_compress CXX)

# Настройка стандарта C++ (используем C++17)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Списки исходных файлов для каждого исполняемого файла
set(SOURCES_PRODUCER src/producer.cpp)
set(SOURCES_CONSUMER src/consumer.cpp)

# Поиск библиотеки zlib (обязательная зависимость)
find_package(ZLIB REQUIRED)

# Сборка исполняемого файла producer
add_executable(producer ${SOURCES_PRODUCER})

# Подключаем заголовочные файлы zlib
target_include_directories(producer PRIVATE ${ZLIB_INCLUDE_DIRS})

# Линкуем с zlib и pthread (для многопоточности)
target_link_libraries(producer PRIVATE ${ZLIB_LIBRARIES} pthread)

# Сборка исполняемого файла consumer
add_executable(consumer ${SOURCES_CONSUMER})

# Аналогичные настройки для consumer
target_include_directories(consumer PRIVATE ${ZLIB_INCLUDE_DIRS})
target_link_libraries(consumer PRIVATE ${ZLIB_LIBRARIES} pthread)

# Дополнительные опции для сборки (можно добавить):
# set(CMAKE_BUILD_TYPE Release)
# target_compile_options(producer PRIVATE -O2 -Wall)
# target_compile_options(consumer PRIVATE -O2 -Wall)
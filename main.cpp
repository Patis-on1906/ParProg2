#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <limits>
#include <mutex>
#include <numeric>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

using Clock = std::chrono::steady_clock;
namespace fs = std::filesystem;

struct Task { std::size_t id; std::uint64_t number; };
struct Result { std::size_t id; std::uint64_t number; std::vector<std::uint64_t> factors; std::size_t worker; };
struct Config { int id; std::string name; int managerDelayMs; std::vector<int> workerDelayMs; int fileDelayMs; };
struct Statistics { std::size_t processed = 0; std::size_t batches = 0; std::size_t written = 0; };

std::vector<std::uint64_t> factorize(std::uint64_t n) {
    std::vector<std::uint64_t> factors;
    if (n == 1) return factors; // 1 не имеет простых множителей
    while (n % 2 == 0) { factors.push_back(2); n /= 2; }
    for (std::uint64_t d = 3; d <= n / d; d += 2) {
        while (n % d == 0) { factors.push_back(d); n /= d; }
    }
    if (n > 1) factors.push_back(n);
    return factors;
}

std::vector<Task> readTasks(const fs::path& input) {
    std::ifstream in(input);
    if (!in) throw std::runtime_error("Не удалось открыть входной файл: " + input.string());
    std::vector<Task> tasks;
    std::string line;
    std::size_t lineNo = 0;
    while (std::getline(in, line)) {
        ++lineNo;
        auto start = line.find_first_not_of(" \t\r");
        if (start == std::string::npos || line[start] == '#') continue;
        std::istringstream parser(line);
        std::uint64_t number = 0;
        std::string trailing;
        if (!(parser >> number) || number == 0 || (parser >> trailing)) {
            throw std::runtime_error("Некорректное число в строке " + std::to_string(lineNo));
        }
        // Явный запрет знака минус
        if (line[start] == '-') throw std::runtime_error("Отрицательное число в строке " + std::to_string(lineNo));
        tasks.push_back({tasks.size() + 1, number});
    }
    return tasks;
}

std::string factorsToString(const std::vector<std::uint64_t>& factors) {
    if (factors.empty()) return "1 (нет простых множителей)";
    std::ostringstream out;
    for (std::size_t i = 0; i < factors.size(); ++i) {
        if (i) out << " * ";
        out << factors[i];
    }
    return out.str();
}

class Simulation {
    struct Mailbox { std::deque<Task> queue; bool busy = false; };
    Config cfg_;
    std::vector<Task> tasks_;
    std::size_t bufferLimit_;
    std::vector<Mailbox> mailboxes_;
    std::vector<std::condition_variable> cvWorkers_;
    std::condition_variable cvSpace_;
    std::mutex queueMutex_;
    bool closed_ = false;
    std::size_t nextChoice_ = 0;
    std::mutex fileMutex_;
    std::mutex logMutex_;
    std::ofstream results_, events_;
    Clock::time_point start_;
    std::vector<Statistics> stats_;
    std::vector<std::thread> threads_;
    std::atomic<std::size_t> totalAssigned_{0};

    void event(std::string who, std::string type, std::size_t taskId = 0, std::string detail = "") {
        const auto us = std::chrono::duration_cast<std::chrono::microseconds>(Clock::now() - start_).count();
        std::lock_guard<std::mutex> lock(logMutex_);
        events_ << us << ';' << who << ';' << type << ';' << taskId << ';' << detail << '\n';
    }
    void pause(int ms) { if (ms > 0) std::this_thread::sleep_for(std::chrono::milliseconds(ms)); }

    void flush(std::size_t id, std::vector<Result>& buffer) {
        if (buffer.empty()) return;
        const auto count = buffer.size();
        event("Worker" + std::to_string(id + 1), "FILE_WAIT", 0, std::to_string(count));
        {
            std::lock_guard<std::mutex> lock(fileMutex_);
            event("Worker" + std::to_string(id + 1), "FILE_LOCK", 0, std::to_string(count));
            pause(cfg_.fileDelayMs); // задержка внутри критической секции моделирует медленный общий файл
            for (const auto& item : buffer) {
                results_ << item.id << ';' << (item.worker + 1) << ';' << item.number << ';'
                         << factorsToString(item.factors) << '\n';
            }
            results_.flush();
            if (!results_) throw std::runtime_error("Ошибка записи файла результатов");
            event("Worker" + std::to_string(id + 1), "FILE_UNLOCK", 0, std::to_string(count));
        }
        ++stats_[id].batches;
        stats_[id].written += count;
        buffer.clear();
    }

    void worker(std::size_t id) {
        auto name = "Worker" + std::to_string(id + 1);
        std::vector<Result> buffer;
        buffer.reserve(bufferLimit_);
        event(name, "START");
        while (true) {
            Task task{};
            {
                std::unique_lock<std::mutex> lock(queueMutex_);
                if (mailboxes_[id].queue.empty() && !closed_) {
                    lock.unlock();
                    event(name, "WAIT_TASK");
                    lock.lock();
                }
                cvWorkers_[id].wait(lock, [&] { return !mailboxes_[id].queue.empty() || closed_; });
                if (mailboxes_[id].queue.empty() && closed_) break;
                task = mailboxes_[id].queue.front();
                mailboxes_[id].queue.pop_front();
                mailboxes_[id].busy = true;
            }
            cvSpace_.notify_one();
            event(name, "RECEIVE", task.id, std::to_string(task.number));
            auto factors = factorize(task.number);
            pause(cfg_.workerDelayMs[id]);
            event(name, "FACTORIZED", task.id, factorsToString(factors));
            buffer.push_back({task.id, task.number, std::move(factors), id});
            ++stats_[id].processed;
            event(name, "BUFFER", task.id, std::to_string(buffer.size()) + "/" + std::to_string(bufferLimit_));
            if (buffer.size() >= bufferLimit_) flush(id, buffer);
            {
                std::lock_guard<std::mutex> lock(queueMutex_);
                mailboxes_[id].busy = false;
            }
            cvSpace_.notify_one();
        }
        flush(id, buffer); // обязательная запись неполного буфера при EOF
        event(name, "STOP");
    }

    // Управляющий поток назначает задачу конкретному исполнителю.
    // У каждого исполнителя допускается не более одной ожидающей задачи.
    void dispatch(const Task& task) {
        std::unique_lock<std::mutex> lock(queueMutex_);
        cvSpace_.wait(lock, [&] {
            return std::any_of(mailboxes_.begin(), mailboxes_.end(), [](const Mailbox& box) { return box.queue.empty(); });
        });
        const auto count = mailboxes_.size();
        std::size_t chosen = count;
        int bestLoad = 3;
        for (std::size_t offset = 0; offset < count; ++offset) {
            std::size_t i = (nextChoice_ + offset) % count;
            if (!mailboxes_[i].queue.empty()) continue;
            int load = mailboxes_[i].busy ? 1 : 0;
            if (load < bestLoad) { bestLoad = load; chosen = i; }
        }
        if (chosen == count) throw std::logic_error("Не найдена свободная очередь");
        mailboxes_[chosen].queue.push_back(task);
        nextChoice_ = (chosen + 1) % count;
        ++totalAssigned_;
        lock.unlock();
        event("Manager", "ASSIGN", task.id, "Worker" + std::to_string(chosen + 1) + ":" + std::to_string(task.number));
        cvWorkers_[chosen].notify_one();
    }

public:
    Simulation(Config cfg, std::vector<Task> tasks, std::size_t bufferLimit, const fs::path& outputDir)
        : cfg_(std::move(cfg)), tasks_(std::move(tasks)), bufferLimit_(bufferLimit),
          mailboxes_(cfg_.workerDelayMs.size()), cvWorkers_(cfg_.workerDelayMs.size()),
          stats_(cfg_.workerDelayMs.size()) {
        if (bufferLimit_ == 0 || mailboxes_.empty()) throw std::invalid_argument("Нужны исполнители и размер буфера > 0");
        fs::create_directories(outputDir);
        results_.open(outputDir / "results.csv", std::ios::trunc);
        events_.open(outputDir / "events.csv", std::ios::trunc);
        if (!results_ || !events_) throw std::runtime_error("Не удалось создать выходные файлы");
        results_ << "task_id;worker_id;number;prime_factors\n";
        events_ << "elapsed_us;actor;event;task_id;details\n";
    }
    void run() {
        start_ = Clock::now();
        for (std::size_t i = 0; i < mailboxes_.size(); ++i)
            threads_.emplace_back(&Simulation::worker, this, i);
        event("Manager", "START", 0, cfg_.name);
        for (const auto& task : tasks_) {
            pause(cfg_.managerDelayMs);
            dispatch(task);
        }
        {
            std::lock_guard<std::mutex> lock(queueMutex_);
            closed_ = true;
        }
        event("Manager", "EOF");
        for (auto& cv : cvWorkers_) cv.notify_one();
        for (auto& thread : threads_) thread.join();
        event("Manager", "JOIN_ALL");
        const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - start_).count();
        std::size_t processed = 0, written = 0, batches = 0;
        std::cout << "\nСценарий " << cfg_.id << " — " << cfg_.name << "\n";
        for (std::size_t i = 0; i < stats_.size(); ++i) {
            processed += stats_[i].processed; written += stats_[i].written; batches += stats_[i].batches;
            std::cout << "  Worker " << (i + 1) << ": задач=" << stats_[i].processed
                      << ", порций=" << stats_[i].batches << ", записей=" << stats_[i].written << '\n';
        }
        std::cout << "  Назначено=" << totalAssigned_ << ", обработано=" << processed
                  << ", записано=" << written << ", порций=" << batches
                  << ", время=" << ms << " мс\n";
        if (processed != tasks_.size() || written != tasks_.size() || totalAssigned_ != tasks_.size())
            throw std::runtime_error("Проверка полноты обработки не пройдена");
    }
};

int main(int argc, char** argv) {
    try {
        if (argc < 2 || argc > 5) {
            std::cerr << "Запуск: " << argv[0]
                      << " <input.txt> [scenario:0..3] [buffer_size] [output_dir]\n"
                      << "  0 = выполнить все три сценария (по умолчанию)\n";
            return 1;
        }
        int scenario = argc >= 3 ? std::stoi(argv[2]) : 0;
        std::size_t bufferSize = argc >= 4 ? std::stoull(argv[3]) : 3;
        fs::path output = argc >= 5 ? argv[4] : "output";
        if (scenario < 0 || scenario > 3 || bufferSize == 0 || bufferSize > 100000)
            throw std::invalid_argument("Неверный сценарий или размер буфера");
        auto tasks = readTasks(argv[1]);
        if (tasks.empty()) throw std::runtime_error("Входной файл не содержит задач");
        std::cout << "Всего задач: " << tasks.size() << ", размер буфера: " << bufferSize << '\n';
        // Ровно три рабочих потока существуют на протяжении каждого сценария.
        const std::vector<Config> configs = {
            {1, "одинаковая скорость", 10, {40,40,40}, 10},
            {2, "второй исполнитель медленнее", 10, {40,200,40}, 10},
            {3, "медленное поступление задач", 160, {40,40,40}, 10}
        };
        for (const auto& cfg : configs) {
            if (scenario != 0 && scenario != cfg.id) continue;
            Simulation simulation(cfg, tasks, bufferSize, output / ("scenario_" + std::to_string(cfg.id)));
            simulation.run();
        }
        std::cout << "Файлы: " << output.string() << "/scenario_N/results.csv и events.csv\n";
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "Ошибка: " << e.what() << '\n';
        return 1;
    }
}

#include <iostream>
#include <string>
#include <numeric>
#include <unordered_map>
#include <algorithm>
#include <memory>
#include <vector>
#include <thread>
#include <fstream>
#include <string.h>
#include <queue>
#include <map>
#include <chrono>

import foo;

using namespace std;

int main(int argc, char **argv) {
    const unsigned int cpus = std::thread::hardware_concurrency();
    string file = "measurements.txt";
    std::vector<std::pair<string, double>> maps[cpus];
    std::queue<std::pair<string, double>> queue;
    std::vector<thread> threads;
    threads.reserve(10);
    bool multiThread = false;

    if (argc > 2)
    {
        file = argv[1];
        multiThread = (string(argv[2]) == "1") ? true : false;
    }

    auto startTotal = std::chrono::system_clock::now();
    ifstream f{file, std::ios::in | std::ios::ate};

    if(!f)
        std::cout << "could not open file" << std::endl;

    unsigned long size = f.tellg();

    char* buffer = new char[size];
    if (!buffer) {
        std::cerr << "Memory allocation failed\n";
        return 1;
    }

    f.seekg(0);
    // Read the content of the file into memory
    if (!f.read(buffer, size)) {
        std::cerr << "Failed to read file\n";
        delete[] buffer;
        return 1;
    }

   //std::cout << file << " " << cpus << " " << size << " bytes" << std::endl;
    auto chunkSize = size / cpus;

    //std::cout << file << " " << cpus << " " << size << " bytes "  << chunkSize << " bytes"<< std::endl;

    auto start = std::chrono::system_clock::now();
    if(!multiThread)
        mapper(buffer, ref(maps[0]), 0, size, 0, size);
    else
    {
        for(uint8_t core = 0; core < cpus; core++)
        {
            unsigned long start = core * chunkSize;
            unsigned long end = (core == cpus - 1) ? end = size : (core + 1) * chunkSize - 1;
                

        //std::cout << "thread " << (uint32_t) core << " Start: " << start << " End: " << end << std::endl;
        threads.emplace_back(mapper, buffer, ref(maps[core]), start, end, core, size);
        }
        auto end = std::chrono::system_clock::now();
        std::chrono::duration<float,std::milli> duration = end - start;
        //std::cout << duration.count() / 1000 << " s" << std::endl;

        for(auto& t : threads)
            t.join();
        threads.clear();
    }

    auto mapperEnd = std::chrono::system_clock::now();
    std::chrono::duration<float,std::milli> mapperDuration = mapperEnd - start;
    std::cout << "Mapper Duration: "<< mapperDuration.count() / 1000 << " s" << std::endl;

    /*std::ofstream out{"out.txt", std::ios::out};
    int res = 1;
    for(auto map : maps)
        for(auto line : map)
        {
            out << line.first << ";" << line.second << std::endl;
            res++;
        }

    std::cout << "Total entries: " << res - 1 << std::endl;*/

/*
    auto chunks = 100000;
    for(auto jobIndex = 0; jobIndex < cpus; jobIndex++)
    {
        //std::cout << jobIndex * chunks << " " << chunks << std::endl;
        threads.push_back(thread(mapper, file, ref(maps[jobIndex]), jobIndex, chunks));
    }
        
    for(auto& t : threads)
        t.join();*/

    int totalEntries = 0;
    for(auto map : maps)
        for(auto line : map)
            totalEntries++;
            //std::cout << line.first << " " << line.second << " " << res ++ << std::endl;

    //std::cout << "Total entries: " << totalEntries << std::endl;


    // suffle
    auto startShuffle = std::chrono::system_clock::now();

    std::map<string, std::vector<double>> shufflemap;
    std::vector<string> keys;
    for(auto map : maps)
        for(auto line : map)
        {
            auto it = shufflemap.find(line.first); 

            if (it == shufflemap.end())
            {
                shufflemap.emplace(line.first, std::vector<double>{line.second}); 
                keys.emplace_back(line.first);
            }
            else
            {
                it->second.push_back(line.second);        
            }   
        }
    
    auto endShuffle = std::chrono::system_clock::now();
    std::chrono::duration<float,std::milli> shuffleDuration = endShuffle - startShuffle;
    std::cout << "Shuffle duration: " << shuffleDuration.count() / 1000 << " s" << std::endl;

    //Reduce
    auto reduceStart = std::chrono::system_clock::now();
    std::map<string, std::tuple<double, double, double>> output;


    if(!multiThread)
        reduce2(ref(keys), ref(shufflemap), ref(output), 0, keys.size());
    else
    {
        size = keys.size() / cpus;
        for(uint8_t core = 0; core < cpus; core++)
        {
            unsigned long start = core * size;
            unsigned long end = (core == cpus - 1) ? end =  keys.size()  : (core + 1) *  size - 1;
            threads.emplace_back(reduce2, ref(keys), ref(shufflemap), ref(output), start, end);
        }

        for(auto& t : threads)
            t.join();
    }
    
    auto reduceEnd = std::chrono::system_clock::now();
    std::chrono::duration<float,std::milli> reduceDuration = reduceEnd - reduceStart;
    std::cout << "Reduce duration: " << reduceDuration.count() / 1000 << " s" << std::endl;

    auto end1 = std::chrono::system_clock::now();
    std::chrono::duration<float,std::milli> totalDuration = end1 - startTotal;

    std::cout << "Total duration: " << totalDuration.count() / 1000 << " s" << std::endl;

    //writeOutput(shufflemap, output, multiThread, cpus, totalEntries, mapperDuration, shuffleDuration, totalDuration);

    return 0;
}
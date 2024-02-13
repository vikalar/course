module;

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

export module foo;

using namespace std;

void writeOutput(std::map<string, std::vector<double>>& shufflemap, std::map<string, std::tuple<double, double, double>>& output, 
                bool multiThread, const unsigned int cpus, const int totalEntries,
                std::chrono::duration<float,std::milli> mapperDuration,
                std::chrono::duration<float,std::milli> shuffleDuration,
                std::chrono::duration<float,std::milli> totalDuration)
{
    ofstream fout{"out-" + to_string(multiThread) + ".txt", std::ios::out}; 
    for(auto [city, values] : shufflemap)
    {
        string str = city + " : " + to_string(values.size()) + " [ " ;
        bool first = true;
        for(auto v : values)
        {
            if(first)
                str += to_string(v);
            else
            {
                str += ", " + to_string(v);
            }
            first = false;

        }
        str += "]";
        

        fout << str << std::endl;
    }
    fout.close();

    ofstream foutSum{"out-summary-" + to_string(multiThread) + ".txt", std::ios::out};
    
    foutSum << "------------------------------" << std::endl;
    foutSum << "Number of cores: " << cpus << std::endl 
    << "Multithread: "  <<  multiThread <<  std::endl 
    << "Total Entries: "  <<  totalEntries <<  std::endl 
    << "Mapper duration: "  <<  mapperDuration.count() / 1000  << " s"  <<  std::endl 
    << "Shuffle duration: " << shuffleDuration.count() / 1000  << " s" <<  std::endl
    << "Reduce duration: " << shuffleDuration.count() / 1000  << " s" <<  std::endl
    << "Total duration: "  <<  totalDuration.count() / 1000  << " s" << std::endl;
    foutSum << "------------------------------" << std::endl;

    for(auto [city, values] : output)
    {
        foutSum <<  city + " : [ "  + to_string(std::get<0>(values)) + "," + to_string(std::get<1>(values)) + "," + to_string(std::get<2>(values)) + "]" << std::endl;
    }
    foutSum.close();
}

double s2d(const std::string& str) {
    double result = 0.0;
    int integerPart = 0;
    double decimalPart = 0.0;
    int decimalCount = 0;
    bool isNegative = false;
    bool inDecimal = false;

    for (char c : str) {
        if (c == '-') {
            isNegative = true;
        } else if (c == '.') {
            inDecimal = true;
        } else {
            if (!inDecimal) {
                integerPart = integerPart * 10 + (c - '0');
            } else {
                decimalPart = decimalPart * 10 + (c - '0');
                decimalCount++;
            }
        }
    }

    result = integerPart + decimalPart / 10; //std::pow(10, decimalCount);

    if (isNegative) {
        result *= -1;
    }

    return result;
}

void shuffle(std::vector<std::pair<string, double>> *maps, std::map<string, std::vector<double>>& map, unsigned int cpus)
{
    for(auto index = 0; index < cpus; index++)
        for(auto line : maps[index])
        {
            if (map.find(line.first) != map.end())
                map.at(line.first).push_back(line.second);
            else
                map.emplace(line.first, line.second); 
        }
}

uint32_t findFirstCharLine(const char* buffer, unsigned long pos)
{
    char character = '\0';
    uint8_t index = 0U;
    uint32_t p = pos;
    
    if((char) buffer[p] == '\n')
        p--;

    while(buffer[p] != '\n')
        p--;
    return p + 1;
}

uint32_t findLastCharLine(const char* buffer, unsigned long pos)
{
    char character = '\0';
    uint8_t index = 0U;
    uint32_t p = pos;
    
    if((char) buffer[p] == '\n')
        p--;

    while(buffer[p] != '\n')
        p--;

    return p;
}

void readLine(const char* buffer, string& line, unsigned long& initPos, unsigned long& size)
{
    line.clear();
    do
    {
        if((char) buffer[initPos] != '\n')
            line += buffer[initPos];
        initPos++;
    } while ( (char) buffer[initPos] != '\n' && initPos < size);

}

export void mapper(const char* buffer, std::vector<std::pair<string, double>>& map, unsigned long start, unsigned long end, int core, unsigned long size)
{
    string line;
    char delim = '\n';
    uint8_t index = 0;  
    unsigned long initPos = 0;

    if(start !=0)
        initPos = findFirstCharLine(buffer, start);
    
    unsigned long lastPos = findLastCharLine(buffer, end);

    auto start2 = std::chrono::system_clock::now();
    while (initPos <= lastPos)
    {   
        readLine(buffer, line, initPos, lastPos);
        if(line.size() > 1)
        {
            size_t pos = line.find(';');
            try
            {
                // I have decided to implement my own s2d, std::stod() has poor performance in multithreading
                map.emplace_back(line.substr(0, pos), s2d(line.substr(pos+1)));
            }
            catch(const std::exception& e)
            {
                std::cerr << e.what() << " " <<line << '\n';
            }
        }
    }
    auto end2 = std::chrono::system_clock::now();
    std::chrono::duration<float,std::milli> duration = end2 - start2;
    //std::cout << "Mapper duration: " << duration.count() / 1000 << std::endl;

    //std::cout << "Thread ID: " << core << " ";
    //std::cout << "Pos Init: "<< initPos <<  " Last Pos: " << lastPos << std::endl;
}

export void reduce2(std::vector<string>& keys, std::map<string, std::vector<double>>& map,  std::map<string, std::tuple<double, double, double>>& output, uint32_t start, uint32_t end)
{
    for(uint32_t index = start; index < end; index++)
    {
        auto size = map[keys[index]].size();
        std::sort(map[keys[index]].begin(), map[keys[index]].end());

        double suma = std::accumulate(map[keys[index]].begin(), map[keys[index]].end(), 0.0);
        double average = suma / size;

        output.emplace(keys[index], std::tuple(map[keys[index]][0], average, map[keys[index]][size - 1]));
    }
}


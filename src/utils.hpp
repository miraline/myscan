#ifndef UTILS_HPP
#define UTILS_HPP

#include <string>
#include <iostream>
#include <chrono>
#include <ratio>
#include <thread>

namespace utils {

std::string trim(const std::string& str, const std::string& whitespace = " \t");

bool endsWith(std::string const &fullString, std::string const &ending);
void replaceStringInPlace(std::string& subject, const std::string& search, const std::string& replace);

void log(const std::string& msg);
void err(const std::string& msg);

class Timer
{
public:
	Timer() {reset();}
	void reset() { begin = std::chrono::high_resolution_clock::now(); }
	double elapsed() const { 
		std::chrono::duration<double, std::milli> ms = std::chrono::high_resolution_clock::now() - begin;
		return ms.count();
	}

private:
	std::chrono::high_resolution_clock::time_point begin;
};

}

#endif

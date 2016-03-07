#include <iostream>
#include <ctime>

#include "utils.hpp"

std::string utils::trim(const std::string& str, const std::string& whitespace)
{
	const auto strBegin = str.find_first_not_of(whitespace);
	if (strBegin == std::string::npos) return ""; // no content

	const auto strEnd = str.find_last_not_of(whitespace);
	const auto strRange = strEnd - strBegin + 1;

	return str.substr(strBegin, strRange);
}

bool utils::endsWith(std::string const &fullString, std::string const &ending) {
	if (fullString.length() >= ending.length()) {
		return (0 == fullString.compare (fullString.length() - ending.length(), ending.length(), ending));
	} else {
		return false;
	}
}

void utils::replaceStringInPlace(std::string& subject, const std::string& search, const std::string& replace) {
	size_t pos = 0;
	while ((pos = subject.find(search, pos)) != std::string::npos) {
		 subject.replace(pos, search.length(), replace);
		 pos += replace.length();
	}
}

static std::string prepareMsg(const std::string& msg) {
	time_t rawtime;
	struct tm * timeinfo;
	char buffer[80];

	time (&rawtime);
	timeinfo = localtime(&rawtime);

	strftime(buffer,80,"%d-%m-%Y %I:%M:%S",timeinfo);
	std::string str(buffer);

	std::string m = str + ": " + msg;

	return m;
}

void utils::log(const std::string& msg) {
	std::string m = prepareMsg(msg);
	std::cout << m << std::endl;
}

void utils::err(const std::string& msg) {
	std::string m = prepareMsg(msg);
	std::cerr << "ERROR: " << m << std::endl;
}

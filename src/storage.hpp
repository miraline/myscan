#ifndef STORAGE_HPP
#define STORAGE_HPP

#include <stdlib.h>
#include <iostream>
#include "mysql_connection.h"
#include "mysql_driver.h"
#include <cppconn/driver.h>
#include <cppconn/exception.h>
#include <cppconn/resultset.h>
#include <cppconn/statement.h>
#include <set>
#include <unordered_set>
#include <unordered_map>
#include <map>
#include <vector>
#include <thread>
#include <libconfig.h++>
#include <signal.h>

//#include <sparsehash/dense_hash_set>
//#include <sparsehash/dense_hash_map>

#define BOOST_SPIRIT_THREADSAFE
#include <boost/foreach.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <boost/filesystem.hpp>

#include "utils.hpp"

using namespace boost::property_tree;
using namespace std;
using namespace libconfig;

namespace MyScan {

typedef int32_t ItemType;
extern ItemType ItemTypeNull; 
//typedef google::dense_hash_set<ItemType> MySet;
typedef set<ItemType> MySet;
//typedef unordered_set<ItemType> MySet; // gcc4.7-4.9 bug results in slow performance!

//typedef google::dense_hash_map<ItemType, size_t> StatMap;
typedef map<ItemType, size_t> StatMap;
extern boost::mutex mtx;

class Table;
class Storage;

struct ColumnCondition {
	enum CompareType { EQ, NE, GT, LT, GE, LE };	// inspired by Perl comparison operators
	enum OperType { Compare, Min, Max };

	const Table &tbl;
	string colName;
	size_t col;
	ColumnCondition(const ptree::value_type &pt, const Table& table, OperType oper);

	CompareType compare;
	OperType oper;
	ItemType val = 0;
	MySet vals;
	bool useSet;

	bool test(ItemType v) const {
		switch (compare) {
			case CompareType::EQ:
				return useSet ? vals.find(v) != vals.end() : v == val;
			case CompareType::NE:
				return useSet ? vals.find(v) != vals.end() : v != val;
			case CompareType::GT:
				return v > val;
			case CompareType::GE:
				return v >= val;
			case CompareType::LT:
				return v < val;
			case CompareType::LE:
				return v <= val;
				throw runtime_error("test(): MIN called");
		}
		throw runtime_error("test(): Unknown condition operator: "+to_string(oper));
	}
	ItemType base(ItemType v) const {
		if (compare == CompareType::EQ) return v;
		return val; //TODO: support useSet
		throw runtime_error("base(): Unknown condition operator: "+to_string(oper));
	}
	string toString() const;
};

struct Request {
	enum Mode { Default, Count };
	Mode mode;
	vector <ColumnCondition> filters, aggregates;
	string indexName, tableName, APIKey;
	size_t offset = 0, limit = 10;

	Request(const ptree &pt, const Storage &storage);
};

struct Response {
	vector<ItemType> ids; // array of fieldName => fieldValue;
	map <string, map<ItemType, ItemType> > count; // fieldName => ( optionValue => count ) 
	map <string, ItemType> min, max;
	size_t rowsMatch, milliSec, activeSlotIndex;
	string info;

	string toJSON(const Request &req) const;
};

struct ColumnMeta {
	string name, type;

	string getName() const {return name;}
	string escapeName() const;
};

class IndexComparator;
class Slot;
class Table {
	protected:
		string data;
		const char* cdata = 0;
		map <string, vector<size_t> > indexes;
		size_t columnsCount = 0;
		size_t rowsCount = 0;
		string name, sql;
		map <string, string> indexStrings;
		vector <ColumnMeta> columns;
		const Slot &slot;

	public:
		Table(const string& tableName, const Slot &s);

		size_t getDataSize() const {return data.size(); }

		size_t getColumnsCount() const { return columns.size(); }
		const ColumnMeta& getColumnByName(const string& columnName) const;
		size_t getColumnIndexByName(const string& columnName) const;

		size_t getRowsCount() const {return rowsCount;}

		void addIndex(const string& indexName, const string& indexStr);

		void appendColumn(const ColumnMeta& col);

		int query(const Request &req, Response &resp) const;

		ItemType getItem(size_t row, size_t col) const {
			return *(ItemType*)(cdata + (row * columnsCount + col) * sizeof(ItemType));
		}

		string getName() const {return name;}

		void appendRow(sql::ResultSet *res);

		void buildIndexes();

		void onDataLoaded();

		void update();

		void setSql(const string &s) {sql = s;}

	protected:
		void setMetaData(sql::ResultSetMetaData *meta);
		void setRowsCount();
		void setColumnsCount(size_t c) {columnsCount = c;}


	friend class Storage;
	friend class IndexComparator;
};

struct IndexComparator {
	const Table & tbl;
	vector <pair<size_t, bool> > sort;

	IndexComparator(const Table &t, const string& indexStr);

	bool operator()(ItemType aRow, ItemType bRow) const {
		for (vector <pair<size_t, bool> >::const_iterator iter = sort.begin(); iter != sort.end(); iter++) {
			ItemType a = tbl.getItem(aRow, iter->first);
			ItemType b = tbl.getItem(bRow, iter->first);
			if (a > b) return iter->second;
			else if (a < b) return !iter->second;
		}
		return false;
	}
};

class Slot {
	protected:
		map <string, Table> tables;
		Storage *storage;
		size_t reqCount = 0;

	public:
		void load();

		void loadConfig();
		void initTables();
		void updateTables();

		const Table& getTableByNameConst(const string &tableName) const;
		Table& getTableByName(const string &tableName);

		int query(const Request &req, Response &resp);

		Storage &getStorage() const {return *storage;}

	friend class Storage;
};

class Storage {
	protected:
		Config cfg;
		Slot slots[2];
		size_t activeSlotIndex = 1; // so that first data are loaded into [0]

		sql::Driver *driver;
		sql::Connection *con = 0;
		//sql::ResultSetMetaData *res_meta;

		bool readyForQuery = false;

		thread updaterThread;

	public:
		bool exitRequested = false;
		static time_t tmSigHup;
		Storage();
		~Storage();

		Slot& activeSlot() { return slots[getActiveSlotIndex()]; }
		const Slot& activeSlotConst() const { return slots[getActiveSlotIndex()]; }
		Slot& inactiveSlot() { return slots[1 - getActiveSlotIndex()]; }
		const Slot& inactiveSlotConst() const { return slots[1 - getActiveSlotIndex()]; }
		size_t getActiveSlotIndex() const;
		void swapSlots();

		const Config& getCfg() const { return cfg; }
		string getCfgStr(const string &key, const string &defVal) const;
		int getCfgInt(const string &key, int defVal) const;

		sql::Connection *connect(bool reconnect=false);

		void checkReady() const;

		void startUpdater();
		void updateWorker();

	protected:
		void loadConfig();

		void disconnect();

	public:
		void update();

		int query(const Request &req, Response &resp);

		static void sigHupHandler(int signum);
};

}

#endif

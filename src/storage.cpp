#include "storage.hpp"

#include <boost/lexical_cast.hpp>
#include <boost/regex.hpp>
#include <boost/algorithm/string/regex.hpp>
#include <limits>
#include <locale>

using namespace MyScan;
using namespace std;
using namespace boost::property_tree;
using namespace utils;
using namespace libconfig;

namespace MyScan {
	boost::mutex mtx;
	ItemType ItemTypeNull = std::numeric_limits<ItemType>::min();
}

ColumnCondition::ColumnCondition(const ptree::value_type &pt, const Table& table, OperType oper) : tbl(table), oper(oper) {
	colName = trim((oper == OperType::Compare) ? (pt.first) : (pt.second.get<string>("")));
	if (! colName.size()) throw runtime_error("empty column provided");

	if (oper == OperType::Compare) {
		if (endsWith(colName, ">=")) {compare = CompareType::GE; colName.pop_back(); colName.pop_back();}
		else if (endsWith(colName, "<=")) {compare = CompareType::LE; colName.pop_back(); colName.pop_back();}
		else if (endsWith(colName, "<>")) {compare = CompareType::NE; colName.pop_back(); colName.pop_back();}
		else if (endsWith(colName, "!=")) {compare = CompareType::NE; colName.pop_back(); colName.pop_back();}
		else if (endsWith(colName, ">")) {compare = CompareType::GT; colName.pop_back();}
		else if (endsWith(colName, "<")) {compare = CompareType::LT; colName.pop_back();}
		else compare = CompareType::EQ;
		colName = trim(colName);
		if (! colName.size()) throw runtime_error("empty column after processing");
	}

	col = tbl.getColumnIndexByName(colName);

	if (oper == OperType::Compare) {
		for (auto& e : pt.second) {
			string vs = e.second.get<string>("");
			if (vs.size()>0) vals.insert( boost::lexical_cast<ItemType>(vs) );
		}
		if (vals.empty()) {
			string vs = pt.second.get<string>("");
			if (vs.size()>0) vals.insert( boost::lexical_cast<ItemType>(vs) );
		}

		if (vals.size() == 0) {
			val = 0;
			useSet = false;
		} else if (vals.size() == 1) {
			val = *(vals.begin());
			useSet = false;
		} else {
			useSet = true;
		}
	}

	//cout << "ColumnCondition: column:" << colName << " col=" << col << " type=" << type << " vals: ";
	//for (auto i=vals.begin(); i!=vals.end(); i++) cout << "[" << *i << "] "; cout << endl;
}

string ColumnCondition::toString() const {
	string ret = colName;
	
	if (oper == OperType::Compare) {
		switch (compare) {
			case CompareType::EQ:
				break;
			case CompareType::NE:
				ret += "<>";
				break;
			case CompareType::GT:
				ret += ">";
				break;
			case CompareType::GE:
				ret += ">=";
				break;
			case CompareType::LT:
				ret += "<";
				break;
			case CompareType::LE:
				ret += "<=";
				break;
		}
	}

	return ret;
}

Request::Request(const ptree &pt, const Storage &storage) {
	ptree::const_assoc_iterator it;
	storage.checkReady();

	indexName = pt.get<string>("index", "id");
	tableName = pt.get<string>("from");

	string api = pt.get<string>("api_key");
	APIKey = string(); for (size_t i=0; i<api.size() && APIKey.size()<=32; i++) if (isalnum(api[i])) APIKey += api[i];

	mode = Mode::Default;
	it = pt.find("mode"); if ( it != pt.not_found() ) {
		string modeString = pt.get<string>("mode");
		if (modeString == "count") mode = Mode::Count;
	}

	offset = pt.get<size_t>("offset", 0);
	limit = pt.get<size_t>("limit", 10);
	if (limit > 100) limit = 100;
	const Table &tbl = storage.activeSlotConst().getTableByNameConst(tableName);

	string name;
	name = "where"; it = pt.find(name); if ( it != pt.not_found() ) {BOOST_FOREACH(const ptree::value_type &v, pt.get_child(name)) { filters.push_back( ColumnCondition(v, tbl, ColumnCondition::OperType::Compare) ); } }
	name = "count"; it = pt.find(name); if ( it != pt.not_found() ) {BOOST_FOREACH(const ptree::value_type &v, pt.get_child(name)) { aggregates.push_back( ColumnCondition(v, tbl, ColumnCondition::OperType::Compare) ); } }
	name = "min"; it = pt.find(name); if ( it != pt.not_found() ) {for (auto &v : pt.get_child(name)) { aggregates.push_back( ColumnCondition(v, tbl, ColumnCondition::OperType::Min) ); } }
	name = "max"; it = pt.find(name); if ( it != pt.not_found() ) {for (auto &v : pt.get_child(name)) { aggregates.push_back( ColumnCondition(v, tbl, ColumnCondition::OperType::Max) ); } }
}

string Response::toJSON(const Request &req) const {
	ptree pt, ptRows, ptCount, ptMin, ptMax;
	for (vector< ItemType >::const_iterator i=ids.begin(); i!=ids.end(); i++) {
		//ptree row;
		//for (map<string,string>::const_iterator j=i->begin(); j!=i->end(); j++) row.put(j->first, j->second);
		ptRows.push_back(make_pair( "", ptree(to_string(*i))));
	}
	pt.put("status", "OK");
	pt.put("timestamp", to_string(time(0)));
	pt.put("api_key", req.APIKey);
	pt.put("from", req.tableName);
	pt.put("index", req.indexName);
	pt.put("offset", to_string(req.offset));
	pt.put("limit", to_string(req.limit));
	pt.put("active_slot", to_string(activeSlotIndex));
	pt.add_child("ids", ptRows);

	for (map<string, map<ItemType,ItemType> >::const_iterator i=count.begin(); i!=count.end(); i++) {
		ptree field;
		for (map<ItemType,ItemType>::const_iterator j=i->second.begin(); j!=i->second.end(); j++) field.put(to_string(j->first), to_string(j->second));
		ptCount.push_back(make_pair(i->first, field));
	}
	pt.add_child("count", ptCount);

	for (map<string, ItemType>::const_iterator mini=min.begin(); mini!=min.end(); mini++) ptMin.put(mini->first, mini->second);
	pt.add_child("min", ptMin);

	for (map<string, ItemType>::const_iterator maxi=max.begin(); maxi!=max.end(); maxi++) ptMax.put(maxi->first, maxi->second);
	pt.add_child("max", ptMax);

	pt.put("total", to_string(rowsMatch));
	pt.put("time_ms", to_string(milliSec));
	ostringstream oss;
	write_json(oss, pt);
	return oss.str();
}

string ColumnMeta::escapeName() const {
	string ret = getName();
	//replaceStringInPlace(ret, ".", "`.`");
	return ret;
}

IndexComparator::IndexComparator(const Table &t, const string& indexStr) : tbl(t) {
	if (indexStr.size() > 0) {
		istringstream cols(indexStr);

		string s;
		while (getline(cols, s, ',')) {
			s = utils::trim(s);
			auto ind = s.find(' ');
			string col, dir = "ASC";
			if (ind == string::npos) col = s;
			else {col = s.substr(0, ind); dir = s.substr(ind+1);}
			size_t colInd = tbl.getColumnIndexByName(col);
			log("  column '" + col + "' " + dir + " #" + to_string(colInd));
			
			sort.push_back( pair<size_t, bool>(colInd, dir==string("DESC") ) );
		}
	} else {
		size_t colInd = 0;
		string col = tbl.columns[colInd].getName();
		log("  column '" + col + "' ASC");
		sort.push_back( pair<size_t, bool>(colInd, false ) );
	}
}

Table::Table(const string& tableName, const Slot &s) : name(tableName), slot(s) {
	addIndex("id", "");
}

const ColumnMeta& Table::getColumnByName(const string& columnName) const {
	for (vector<ColumnMeta>::const_iterator i=columns.begin(); i!=columns.end(); i++) if (i->name==columnName) return *i;
	throw runtime_error("getColumnByName: column not found: "+columnName);
}

size_t Table::getColumnIndexByName(const string& columnName) const {
	size_t ind = 0;
	for (vector<ColumnMeta>::const_iterator i=columns.begin(); i!=columns.end(); i++, ind++) if (i->name==columnName) return ind;
	throw runtime_error("getColumnIndexByName: column not found: "+columnName);
}

void Table::addIndex(const string& indexName, const string& indexStr) {
	indexStrings[indexName] = indexStr;
}

void Table::appendColumn(const ColumnMeta& col) {
	columns.push_back(col);
}

void Table::setRowsCount() { rowsCount = columnsCount > 0 ? getDataSize() / (sizeof(ItemType) * columnsCount) : 0; }

void Table::appendRow(sql::ResultSet *res) {
	for (size_t i=0; i < columnsCount; i++) {
		ItemType val = res->getInt(i+1);
		data += string((char*)&val, sizeof(ItemType));
	}
}

void Table::onDataLoaded() {
	cdata = data.c_str();

	setRowsCount();
	//log("rows: " + to_string(rowsCount));

	buildIndexes();
}

void Table::buildIndexes() {
	log("Building indexes..");

	for (map <string, string>::const_iterator si = indexStrings.begin(); si != indexStrings.end(); si++) {
		string indexName = si->first;
		string indexStr = si->second;
		log("index '" + indexName + "':");

		vector<size_t> &index = indexes[indexName];
		index.resize(rowsCount);
		for (size_t i=0; i<rowsCount; i++) index[i] = i;

		IndexComparator cmp(*this, indexStr);
		sort(index.begin(), index.end(), cmp);
	}
	//log("buildIndexes: done");
}

int Table::query(const Request &req, Response &resp) const {
	//boost::posix_time::ptime mst1 = boost::posix_time::microsec_clock::local_time();
	size_t cols = getColumnsCount();
	register size_t aggrInd, aggrNum, aggrSize = req.aggregates.size();
	size_t filtersTotal = 0;
	register size_t filtersMatch;
	resp.rowsMatch = 0;

	vector < StatMap > intStats(aggrSize); // field => ( optionValue => count ) 
	vector < ItemType > minStats(cols), maxStats(cols);
	vector < vector <size_t> > colAggregates(cols); // columns => aggregrates

	for (size_t fi=0; fi<req.filters.size(); fi++) filtersTotal++;
	for (size_t ai=0; ai<req.aggregates.size(); ai++) colAggregates[ req.aggregates[ai].col ].push_back( ai );
	for (size_t ci=0; ci<cols; ci++) { minStats[ci] = maxStats[ci] = ItemTypeNull; }

	map<string, vector<size_t> >::const_iterator indexIter = indexes.find(req.indexName);
	if (indexIter == indexes.end()) throw runtime_error("Index not found: "+req.indexName);
	const vector <size_t> & index = indexIter->second;
	register vector<size_t>::const_iterator iInd, iEnd = index.end();
	register size_t /*row, */failCol;
	register ItemType val;
	register vector<ColumnCondition>::const_iterator filtIter, filtBegin = req.filters.begin(), filtEnd = req.filters.end();

	for (iInd = index.begin(); iInd != iEnd; ++iInd) {
		//row = *iInd;

		filtersMatch = 0;
		for (filtIter = filtBegin; filtIter != filtEnd; ++filtIter) {
			val = getItem(*iInd, filtIter->col);
			if (filtIter->test(val)) {
				++filtersMatch;
			} else {
				if (req.mode==Request::Mode::Count) break;
				else failCol = filtIter->col;
			}
		}

		if (filtersMatch == filtersTotal) {
			if (req.mode == Request::Mode::Default) {
				for (aggrInd = 0; aggrInd < aggrSize; ++aggrInd) {
					register const ColumnCondition &condition = req.aggregates[ aggrInd ];
					val = getItem(*iInd, condition.col);
					if (condition.oper == ColumnCondition::OperType::Min) {
						if (val>0 && (minStats[condition.col]==ItemTypeNull || minStats[condition.col] > val)) minStats[condition.col] = val;
					} else if (condition.oper == ColumnCondition::OperType::Max) {
						if (val>0 && (maxStats[condition.col]==ItemTypeNull || maxStats[condition.col] < val)) maxStats[condition.col] = val;
					} else {
						if ( condition.test(val) ) intStats[ aggrInd ][ condition.base(val) ]++;
					}
				}

				if (resp.rowsMatch >= req.offset && resp.rowsMatch < req.offset + req.limit) resp.ids.push_back( getItem(*iInd, 0) );
			}
			++resp.rowsMatch;

		} else if (req.mode==Request::Mode::Default && filtersMatch + 1 == filtersTotal) {
			if (colAggregates[failCol].size() > 0) {
				for (aggrNum = 0; aggrNum < colAggregates[failCol].size(); ++aggrNum) {
					aggrInd = colAggregates[failCol][aggrNum];
					register const ColumnCondition &condition = req.aggregates[ aggrInd ];
					val = getItem(*iInd, condition.col );
					if (condition.oper == ColumnCondition::OperType::Min) {
						if (val>0 && (minStats[condition.col]==ItemTypeNull || minStats[condition.col] > val)) minStats[condition.col] = val;
					} else if (condition.oper == ColumnCondition::OperType::Max) {
						if (val>0 && (maxStats[condition.col]==ItemTypeNull || maxStats[condition.col] < val)) maxStats[condition.col] = val;
					} else {
						if ( condition.test(val) ) intStats[ aggrInd ][ condition.base(val) ]++;
					}
				}
			}
		}
	}

	for (aggrInd = 0; aggrInd < aggrSize; aggrInd++) {
		register const ColumnCondition &condition = req.aggregates[ aggrInd ];
		if (condition.oper == ColumnCondition::OperType::Min) {
			resp.min[ condition.toString() ] = minStats[ condition.col ];
		} else if (condition.oper == ColumnCondition::OperType::Max) {
			resp.max[ condition.toString() ] = maxStats[ condition.col ];
		} else {
			for (auto sj = intStats[aggrInd].begin(); sj != intStats[aggrInd].end(); sj++) {
				resp.count[ condition.toString() ][ sj->first ] = sj->second; // SI??
			}
		}
	}

	resp.activeSlotIndex = slot.getStorage().getActiveSlotIndex();
	
	//boost::posix_time::ptime mst2 = boost::posix_time::microsec_clock::local_time();
	//boost::posix_time::time_duration msdiff = mst2 - mst1;
	//cout << "Query processed in " << msdiff.total_milliseconds() << "ms returning " << resp.ids.size() << " out of " << rowsMatch << " matched rows" << endl;
	return 0;
}

void Table::update() {
	log("Loading table '" + name + "' ... ");
	log(string("  SQL query: '" + sql + "'"));

	sql::ResultSet *res;
	sql::Statement *stmt;

	sql::Connection *con = slot.getStorage().connect();
	
	stmt = con->createStatement();
	try {
		res = stmt->executeQuery(sql.c_str());
	} catch(sql::SQLException& e) {
		if (e.getErrorCode() != 2006) throw runtime_error("Table::update(): could not execute SQL: "+sql+"\nError: "+e.what()+" (MySQL error code: " + to_string(e.getErrorCode()) + ", SQLState: " + e.getSQLState() + " )" );
		log("Connection lost");
		con = slot.getStorage().connect(true);
		delete stmt;
		stmt = con->createStatement();
		res = stmt->executeQuery(sql.c_str());
	}
	setMetaData( res->getMetaData() );

	while (res->next()) appendRow(res);
	delete res;
	delete stmt;

	onDataLoaded();

	log("Loaded table '" + name + "': " + to_string( getColumnsCount() ) + " cols, " + to_string( getDataSize() ) + " bytes, " + to_string( getRowsCount() ) + " rows, " + to_string(indexes.size()) + " indexes");
}

void Table::setMetaData(sql::ResultSetMetaData *meta) {

	setColumnsCount(meta->getColumnCount());

	for (size_t i = 1; i <= columnsCount; i++) {
		ColumnMeta col;
		col.name = meta->getColumnLabel(i);
		col.type = meta->getColumnTypeName(i);

		columns.push_back(col);
	}
}

void Slot::load() {
	initTables();

	updateTables();
}

void Slot::initTables() {
	tables.clear();

	const Setting &root = getStorage().getCfg().getRoot();
	
	const Setting &tbls = root["tables"];

	for (int i=0; i<tbls.getLength(); i++) {
		const Setting &t = tbls[i];

		Table & tbl = getTableByName( t.lookup("name" ) );
		tbl.setSql( t.lookup("sql") );

		const Setting &inds = t["indexes"];
		for (int j=0; j<inds.getLength(); j++) {
			tbl.addIndex(inds[j].lookup("name"), inds[j].lookup("columns"));
		}
	}
}

void Slot::updateTables() {
	for (map <string, Table>::iterator ti=tables.begin(); ti!=tables.end(); ti++) ti->second.update();
}

int Slot::query(const Request &req, Response &resp) {
	int ret = -1;

	mtx.lock(); reqCount++; mtx.unlock();

	try {
		const Table & tbl = getTableByNameConst(req.tableName);
		ret = tbl.query(req, resp);
	} catch (...) {
		reqCount--;
		throw;
	}

	mtx.lock(); reqCount--; mtx.unlock();

	return ret;
}

const Table& Slot::getTableByNameConst(const string &tableName) const {
	map<string, Table>::const_iterator i = tables.find(tableName);
	if (i != tables.end()) return i->second;
	throw runtime_error("getTableByNameConst(): table not found: " + tableName);
}

Table& Slot::getTableByName(const string &tableName) {
	map<string, Table>::iterator i = tables.find(tableName);
	if (i != tables.end()) return i->second;

	pair<map<string, Table>::iterator, bool> res = tables.insert( pair<string,Table>(tableName, Table(tableName, *this) ) );
	if (! res.second) throw runtime_error("getTableByName(): could not insert table: " + tableName);

	return res.first->second;
}

Storage::Storage() {
	for (int i=0; i<2; i++) slots[i].storage = this;

	loadConfig();

	connect();

	startUpdater();
}

Storage::~Storage() {
	disconnect();
}

void Storage::loadConfig() {
	try { 
		cfg.readFile("myscan.cfg");
	} catch(const FileIOException &e) { 
		err(string("Error while reading config file"));
		throw e;
	} catch(const ParseException &e) { 
		err(string("Config parse error at ") + e.getFile() + ":" + to_string( e.getLine() ) +  " - " + e.getError() );
		throw e;
	}

	//const Setting& root = cfg.getRoot();
}


string Storage::getCfgStr(const string &key, const string &defVal) const {
	try {
		return string( (const char*)getCfg().lookup(key) );
	} catch (const SettingNotFoundException &nfex) {}
	return defVal;
}
int Storage::getCfgInt(const string &key, int defVal) const {
	try {
		return getCfg().lookup(key);
	} catch (const SettingNotFoundException &nfex) {}
	return defVal;
}

sql::Connection* Storage::connect(bool reconnect) {
	if (reconnect && con) disconnect();
	else if (con) return con;

	driver = get_driver_instance();

	for (size_t i=0; true; i++) {
		try {
			log("Connecting to MySQL... ");
			con = driver->connect("unix:///var/run/mysqld/mysqld.sock", "realty", "superXdom1991");
			//con = driver->connect("tcp://127.0.0.1:3306", "realty", "superXdom1991");
			bool myTrue = true;
			con->setClientOption("OPT_RECONNECT", &myTrue);
			con->setSchema("realty");
		} catch (sql::SQLException &e) {
			err(string("# CONN ERR: ") + e.what() + " (MySQL error code: " + to_string(e.getErrorCode()) + ", SQLState: " + e.getSQLState() + " )");

			if (i > 0) this_thread::sleep_for(chrono::milliseconds( 100 )); if (i > 10) this_thread::sleep_for(chrono::milliseconds( 1000 ));
			//if (i >= 100) throw e;
			continue;
		}
		break;
	}
	log("Connected.");

	return con;
}

void Storage::disconnect() {
	if (con) {
		delete con;
		con = 0;
	}
}

void Storage::swapSlots() {
	mtx.lock();
	activeSlotIndex = 1 - activeSlotIndex;
	mtx.unlock();
	if (! readyForQuery) readyForQuery = true;
}

size_t Storage::getActiveSlotIndex() const {
	size_t ret;
	mtx.lock();
	ret = activeSlotIndex;
	mtx.unlock();
	return ret;
}

void Storage::update() {
	loadConfig();

	inactiveSlot().load();
	Slot &slot = activeSlot();

	swapSlots();

	for (size_t i=0; true; i++) {
		mtx.lock();
		size_t cnt = slot.reqCount;
		mtx.unlock();
		if (! cnt) break;
		if (i % 100 == 0) log("Waiting for requests to leave inactive slot # " + to_string(cnt) + " i=" + to_string(i));
		this_thread::sleep_for(chrono::milliseconds( 20 ));
	}
	inactiveSlot().tables.clear();
	log("Swapped to slot # " + to_string( getActiveSlotIndex() ) );
}

void Storage::checkReady() const {
	while (! readyForQuery ) {
		log("Waiting for data...");
		this_thread::sleep_for(chrono::milliseconds( 200 ));
	}
}

int Storage::query(const Request &req, Response &resp) {
	return activeSlot().query(req, resp);
}

void Storage::startUpdater() {
	log("Starting updater thread");
	updaterThread = std::thread( [&] { updateWorker(); } );
}

void Storage::updateWorker() {
	time_t tm, tm2, sl, period = 30;

	log("Updater started");

	while (! exitRequested) {
		//cout << "update_worker()" << endl;

		tm = tm2 = time(0);

		try {
			update();
			tm2 = time(0);
			log("Tables updated in " + to_string(tm2-tm) +  "s");
			period = getCfgInt("update_period", period);
		} catch (runtime_error &e) {
			err(string("Updater Runtime Error: ") + e.what());
		} catch (exception &e) {
			err(string("Updater Exception: ") + e.what());
		} catch (...) {
			err("Updater Unknown exception");
		}

		this_thread::sleep_for( chrono::milliseconds( 3000 ) );

		if (tm2 < period + tm) {
			sl = period - (tm2 - tm);
			log("Sleeping for " + to_string(sl) + "s");
			for (time_t i = 0; i < sl * 2; i++) {
				if (time(0) < MyScan::Storage::tmSigHup + 2) break;
				this_thread::sleep_for( chrono::milliseconds( 500 ) );
			}
		}
	}
}

time_t Storage::tmSigHup = 0;
void Storage::sigHupHandler(int signum) {
	if (signum==SIGHUP) {tmSigHup = time(0); log("Received SIGHUP");}
}


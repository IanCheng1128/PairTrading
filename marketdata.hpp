//
//  marketdata.hpp
//

#ifndef marketdata_hpp
#define marketdata_hpp

#include <string>
#include <vector>
#include <map>
#include <set>
#include <cmath>
#include <numeric>
#include <algorithm>
#include <iomanip>

#include "database.hpp"
#include "curl/curl.h"


using namespace std;

int RetrieveMarketData(string url_request, string & read_buffer);
int PopulateStockTable(sqlite3 *db, set<string> Tickers, string table_name, string start_time, string end_time);
int PopulatePairTable(sqlite3 *db, const map<int, pair<string, string> > & symbols);

#endif

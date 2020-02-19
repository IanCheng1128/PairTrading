//
//  marketdata.cpp
//

#include "marketdata.hpp"
#include "database.hpp"
#include <string>
#include <iostream>
#include <sstream>
#include <vector>
#include <set>
using namespace std;

static size_t WriteCallback(void *contents, size_t size, size_t nmemb, void *userp)
{
	((std::string*)userp)->append((char*)contents, size * nmemb);
	return size * nmemb;
}

int RetrieveMarketData(string url_request, string & read_buffer)
{
	curl_global_init(CURL_GLOBAL_ALL);
	CURL * myHandle;
	CURLcode result;
	myHandle = curl_easy_init();

	curl_easy_setopt(myHandle, CURLOPT_URL, url_request.c_str());

	//adding a user agent
	curl_easy_setopt(myHandle, CURLOPT_USERAGENT, "Mozilla/5.0 (Windows; U; Windows NT 6.1; rv:2.2) Gecko/20110201");
	curl_easy_setopt(myHandle, CURLOPT_SSL_VERIFYPEER, 0);
	curl_easy_setopt(myHandle, CURLOPT_SSL_VERIFYHOST, 0);
	//curl_easy_setopt(myHandle, CURLOPT_VERBOSE, 1);

	// send all data to this function  
	curl_easy_setopt(myHandle, CURLOPT_WRITEFUNCTION, WriteCallback);

	// we pass our 'chunk' struct to the callback function 
	curl_easy_setopt(myHandle, CURLOPT_WRITEDATA, &read_buffer);

	//perform a blocking file transfer
	result = curl_easy_perform(myHandle);

	// check for errors 
	if (result != CURLE_OK) {
		fprintf(stderr, "curl_easy_perform() failed: %s\n", curl_easy_strerror(result));
		return -1;
	}
	curl_easy_cleanup(myHandle);
	return 0;
}

int PopulatePairTable(sqlite3 *db, const map<int, pair<string, string> > & pairs)
{
    for (map<int, pair<string, string> >::const_iterator itr = pairs.begin(); itr != pairs.end(); itr++)
    {
        char pair_insert_table[512];
        sprintf(pair_insert_table, "INSERT INTO PAIRS (Id, Tickers1, Tickers2) VALUES(%d, \"%s\", \"%s\")", itr->first, itr->second.first.c_str(), itr->second.second.c_str());
        if (InsertTable(pair_insert_table, db) == -1)
            return -1;
    }
    return 0;
}


int PopulateStockTable(sqlite3 *db, set<string> Tickers, string table_name, string start_time, string end_time){
    //Retrieve market data
    for (set<string>::const_iterator itr = Tickers.begin(); itr != Tickers.end(); ++itr)
    {
        cout << *itr << endl;
        string readBuffer;
        string stock_url_common = "https://eodhistoricaldata.com/api/eod/";
        string stock_start_date = start_time;
        string stock_end_date = end_time;
        string api_token = "5ba84ea974ab42.45160048";
        string pair_stock_retrieve_url = stock_url_common + *itr + ".US?" +
            "from=" + stock_start_date + "&to=" + stock_end_date + "&api_token=" + api_token + "&period=d";
        if (RetrieveMarketData(pair_stock_retrieve_url, readBuffer) == -1)
            return -1;
        cout << readBuffer << endl;
        
        //Insert data into the table Tickers;
        string line, word;
        vector<string> row;
        stringstream ss(readBuffer);
        getline(ss, line, '\n');
        
        while(true){
            getline(ss, line);
            if(ss.fail()) break;
            cout << line << endl;
            row.clear();
            stringstream sss(line);
            while(getline(sss, word, ',')){
//                cout << word << endl;
                row.push_back(word);
            }
            cout << "##############################" << endl;
            cout << "Ticker name" << " " << *itr << endl;
            cout << "row size"<< row.size() << endl;
            if (row.size() == 1)
            {
                cout << "sssssss" << endl;
                continue;
            }
            if(row.size() > 1)
            {
//                cout << row[0] << " " << row[1]<< " " << row[2]<< " " << row[3]<< " " << row[4]<< " " << row[5]<< " " << row[6] << endl;
                char sql[128];
                if(row[0]!="" && row[1]!="" && row[2]!="" && row[3]!="" && row[4]!="" && row[5]!="" && row[6]!="")
                {
                    sprintf(sql, "insert into %s\
                            values (\'%s\', \'%s\', round(%f, 2), round(%f, 2), round(%f, 2), round(%f, 2), round(%f, 2), %d);", table_name.c_str(), (*itr).c_str(), row[0].c_str(), stof(row[1]),stof(row[4]), stof(row[5]), stof(row[2]), stof(row[3]), stoi(row[6]));
                    if(InsertTable(sql, db) == -1){
                        return -1;
                    }
                }
            }
        }
//        if(next(itr) == Tickers.end()){
//            break;
//        }
        cout << "SON OF A BITCH!" << endl;
    }
    cout <<"SON OF A BITCH!<<<<<<<<<<<<<<<<<<<<<<<" << endl;
    return 0;
}

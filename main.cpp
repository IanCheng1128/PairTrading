//
//  main.cpp
//  Pair_Traing_project
//
//  Created by IanCheng on 2/10/20.
//  Copyright © 2020 NYU. All rights reserved.
//

#include <stdio.h>
#include <iostream>
#include <string>
#include <vector>
#include <map>
#include <set>
#include <cmath>
#include <fstream>
#include <sstream>
#include "database.hpp"
#include "marketdata.hpp"

using namespace std;

void create_Pairs_table(sqlite3 * & db, map<int, pair<string, string> > Pairs){
    const char* s = "create table Pairs\
                (Id int,\
                Tickers1 varchar(20),\
                Tickers2 varchear(20));";
    CreateTable(s, db);
    
    PopulatePairTable(db, Pairs);
}

void create_Tickers1_table(sqlite3 * & db){
    const char* s = "create table Tickers1 AS\
                    select distinct Pairs.Tickers1 from Pairs;";
    CreateTable(s, db);
}

void create_Tickers2_table(sqlite3 * & db){
    const char* s ="create table Tickers2 AS\
                    select distinct Pairs.Tickers2 from Pairs;";
    CreateTable(s, db);
}

void create_stocks1_table(sqlite3 * & db, set<string> Tickers1, string start_time, string end_time){
    char sqllines[128];
    sprintf(sqllines, "create table Prices1\
            (Symbol varchar(20), Date varchar(20), Open double, Close double, AdjClose double,High double, Low double, Volume int);");
    CreateTable(sqllines, db);
    
    PopulateStockTable(db, Tickers1, "Prices1", start_time, end_time);
}

void create_stocks2_table(sqlite3 * & db, set<string> Tickers2, string start_time, string end_time){
    char sqllines[128];
    sprintf(sqllines, "create table Prices2\
            (Symbol varchar(20), Date varchar(20), Open double, Close double, AdjClose double,High double, Low double, Volume int);");
    CreateTable(sqllines, db);
    
    PopulateStockTable(db, Tickers2, "Prices1", start_time, end_time);
}

void create_Trades_table(sqlite3 * & db){
    const char* s = "create table Trades\
    (PairId int,\
    Date varchar(20),\
    Profit double;";
    
    CreateTable(s, db);
}

void create_Pair_Prices_Ratio_table(sqlite3 * & db){
    const char* s = "create table PairPriceRatio as\
    SELECT Pairs.Id, Pairs.Tickers1, Pairs.Tickers2, round(StDev(Prices1.Adj_Close/Prices.Adj_Close), 2) AS PriceRatio\
    FROM Prices1, Prices2, Pairs\
    WHERE Prices1.Symbol=Pairs.Ticker1 and Prices2.Symbol=Pairs.Ticker2 and Prices1.Date=Prices2.Date \
    GROUPBY Pairs.Id, Pairs.Tickers1, Pairs.Tickers2\
    ORDERBY Pairs.Id, Pairs.Tickers1, Pairs.Tickers2;";
    
    CreateTable(s, db);
}

void create_Pair_test_price_table(sqlite3 * & db){
    const char* s = "create table PairTestPrice as\
    SELECT Pairs.Id, Pairs.Tickers1 AS Tickers1, Pairs.Tickers2 AS Tickers2, TestPrices1.Open AS Tickers1Open, TestPrices1.Close AS Tickers1Close, TestPrices2.Open AS Tickers2Open, TestPrices2.Close AS Tickers2Close, TestPrices2.Date AS PriceDate\
    FROM TestPrices1,TestPrices2,Pairs\
    WHERE TestPrices1.Symbol=Pairs.Tickers1 and TestPrices2.Symbol=Pairs.Tickers2 and TestPrices1.Date=TestPrices2.Date\
    ORDER BY Pairs.Tickers1, Pairs.Tickers2, TestPrices1.Date;";
    
    CreateTable(s, db);
}

void create_Profitable_trades_table(sqlite3 * & db){
    const char* s = "create table ProfitableTrades as\
    SELECT Pairs.Id, Pairs.Tickers1, Pairs.Tickers2,\
    sum(iif(Trades.Profit > 0, 1,0)) AS ProfitableTrades,\
    sum(iif(Trades.Profit < 0, 1,0)) AS LossTrades,\
    sum(iif(Trades.Profit <>0, 1, 0)) AS TotalTrades, round(ProfitableTrades/LossTrades, 2) AS ProfitableRatio\
    FROM Pairs, Trades\
    WHERE Pairs.Id=Trades.PairId\
    GROUPBY Pairs.Id,Pairs.Tickers1,Pairs.Tickers2;";
    
    CreateTable(s, db);
}



int main(){
//  Create 3 tables: Pairs, Tickers1, Tickers2
    map<int, pair<string, string> > Pairs;
    set<string> Tickers1;
    set<string> Tickers2;

//  Read the pair file into Pair table
    fstream fin;
    fin.open("PairTrading.csv",ios::in);
    vector<string> row;
    string line, word, temp;
    int count = 0;
    while(getline(fin, line, '\n')){
        row.clear();
        stringstream ss(line);
        while(getline(ss, word, ',')){
            row.push_back(word);
        }
        Pairs.insert({++count, pair<string, string>(row[0],row[1])});
        Tickers1.insert(row[0]);
        Tickers2.insert(row[1]);
    }
    fin.close();
    
    for (map<int, pair<string, string> >::const_iterator itr = Pairs.begin(); itr != Pairs.end(); itr++)
    {
        cout << itr->first<<" "<<itr->second.first<<" " << itr->second.second << endl;
    }


//  Create a database to store Pairs, Tickers1, Tickers2
    sqlite3 *db;
    OpenDatabase("/Users/iancheng/xcode/FRE7831_Project_Spring_2019_Mac/Pair_Trading_project/PairTrading.db", db);
    create_Pairs_table(db, Pairs);
    create_Tickers1_table(db);
    create_Tickers2_table(db);

//  Create Prices1 and Prices2 tables
    create_stocks1_table(db, Tickers1, "2018-01-02", "2018-12-31");
    create_stocks2_table(db, Tickers2, "2018-01-02", "2018-12-31");

//  Create PairPriceRatio table
    create_Pair_Prices_Ratio_table(db);

//  Create TestPrice table
    PopulateStockTable(db, Tickers1, "TestPrice1", "2019-01-02", "2019-12-31");
    PopulateStockTable(db, Tickers2, "TestPrice2", "2019-01-02", "2019-12-31");

//  Create PairTest table
    create_Pair_test_price_table(db);

//  Create Trades table
    vector<string> Date;
    vector<int> Pairs_list;
    vector<double> Tickers1_open;
    vector<double> Tickers1_close;
    vector<double> Tickers2_open;
    vector<double> Tickers2_close;
    
//  Store the std values of different pairs
    map<int, double> std_map;
//  Store the data of Trades table
    map<int, pair<string, double> > Trades_map;

//  Get the Pairs_list, Date, Tickers1, Tickers2
    int rc = 0;
    char *error = NULL;
    cout << "Retrieving values in a table ..." << endl;
    char **results = NULL;
    int rows, columns;
    const char* sql_select = "select * from PairTestPrice;";
    sqlite3_get_table(db, sql_select, &results, &rows, &columns, &error);
    if (rc)
    {
        cerr << "Error executing SQLite3 query: " << sqlite3_errmsg(db) << endl << endl;
        sqlite3_free(error);
        return -1;
    }
    for (int rowCtr = 0; rowCtr <= rows; ++rowCtr)
    {
        if(rowCtr == 0) continue;
        for (int colCtr = 0; colCtr < columns; ++colCtr)
        {
            int cellPosition = (rowCtr * columns) + colCtr;
            if(colCtr == 0){
                Pairs_list.push_back(stoi(results[cellPosition]));
            }
            else if(colCtr == 3){
                Tickers1_open.push_back(stof(results[cellPosition]));
            }
            else if(colCtr == 4){
                Tickers1_close.push_back(stof(results[cellPosition]));
            }
            else if(colCtr == 5){
                Tickers2_open.push_back(stof(results[cellPosition]));
            }
            else if(colCtr == 6){
                Tickers2_close.push_back(stof(results[cellPosition]));
            }
            else if(colCtr == 7){
                Date.push_back(results[cellPosition]);
            }
        }
    }
    
//  Get the std of Pairs
    int rc1 = 0;
    char *error1 = NULL;
    cout << "Retrieving values in a table ..." << endl;
    char **results1 = NULL;
    int rows1, columns1;
    const char* sql_select1 = "select Id, PriceRatio from PairPriceRatio;";
    sqlite3_get_table(db, sql_select1, &results1, &rows1, &columns1, &error1);
    if (rc1)
    {
        cerr << "Error executing SQLite3 query: " << sqlite3_errmsg(db) << endl << endl;
        sqlite3_free(error1);
        return -1;
    }
    for (int rowCtr = 0; rowCtr <= rows; ++rowCtr)
    {
        if(rowCtr == 0) continue;
        int id;
        double std;
        for (int colCtr = 0; colCtr < columns; ++colCtr)
        {
            int cellPosition = (rowCtr * columns) + colCtr;
            if(colCtr == 0){
                id = stoi(results1[cellPosition]);
            }
            else std = stof(results1[cellPosition]);
        }
        std_map.insert({id, std});
    }
    
//  Calculate the Profit
    vector<double> Profit;
    const int N1 = 10000;
    int N2;
    for(int i=1;i<Date.size();++i){
        double signal = abs(Tickers1_close[i-1] / Tickers2_close[i-1] - Tickers1_open[i] / Tickers2_open[i]);
        N2 = N1 * (Tickers1_open[i] / Tickers2_open[i]);
        if (signal > std_map[Pairs_list[i]]){
            Profit.push_back(N1 * (Tickers1_open[i] - Tickers1_close[i]) + N2 * (Tickers2_open[i] - Tickers2_close[i]));
        }
        else if(signal < std_map[Pairs_list[i]]){
            Profit.push_back(-N1 * (Tickers1_open[i] - Tickers1_close[i]) - N2 * (Tickers2_open[i] - Tickers2_close[i]));
        }
    }
    
//  Populate data into Trades table
    for(int i=0;i<Date.size();++i){
        char s[128];
        sprintf(s, "insert into Trades\
                values (%d, /'%s/', round(%f, 2));", Pairs_list[i], Date[i].c_str(), Profit[i]);
        InsertTable(s, db);
    }
    
//  Create profitable trades table
    create_Profitable_trades_table(db);
    
    DisplayTable("select * from ProfitableTrades", db);
    

    CloseDatabase(db);
}


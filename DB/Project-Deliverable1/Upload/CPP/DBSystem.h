#ifndef DBSystem_H
#define DBSystem_H
#include <string>

using namespace std;

class DBSystem
{
	public:
	void readConfig(string str);
	void populateDBInfo();
	string getRecord(string tableName, int recordId);
	void insertRecord(string tableName, string record);
};

#endif

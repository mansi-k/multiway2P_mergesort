#include <iostream>
#include <fstream>
#include <sstream>
#include <sys/stat.h>
#include <vector>
#include <cstdio>
using namespace std;

vector<int> COL_ORDER;

//template <typename kt>
//struct CustomLessThan {
//    bool operator()(tuple<T1, T2, T3> const &tup1, tuple<T1, T2, T3> const &tup2) const {
//        return std::get<1>(lhs) < std::get<1>(rhs);
//    }
//};

template <typename T>
class RecCompare {
public:
    int operator()(T &r1, T &r2) {
        for(int i=0;i<COL_ORDER.size();i++) {
            if(r1.get(i).equals(r2.get(i)))
                continue;
            return (-1*r1.get(i).compareTo(r2.get(i)));
        }
    }
};

class TwoPhaseMergeSort {
public:
    vector<int> sortColIndexVec;
    string inputFile;
    string outputFile;
    long int maxMemSize=0, lineSize=0;
    string order;
    vector<string> sortColsVec;
    vector<string> allColNamesVec;
    vector<string> tempFilesVec;
    vector<ifstream*> openTempFilesVec;

    TwoPhaseMergeSort(string inFile, string outFile, int mem_size, string ord, vector<string> &sort_cols_vec) {
        inputFile = inFile;
        outputFile = outFile;
        maxMemSize = (long int) mem_size*1000000*0.8;
        order = ord;
        sortColsVec = sort_cols_vec;
        getMetadata();
    }

    void getMetadata() {
        ifstream metafile("metadata.txt");
        string colname, line;
        long int colsize;
        while(getline(metafile, line)) {
            int cpos = line.find(',');
            if (cpos == string::npos) {
                cout << "Invalid format of Metadata!" << endl;
                exit(0);
            }
            colname = line.substr(0,cpos);
            colsize = stol(line.substr(cpos+1));
            allColNamesVec.push_back(colname);
            lineSize += colsize;
        }
        metafile.close();
        for(auto it=sortColsVec.begin();it!=sortColsVec.end();it++) {
            auto jt = find(allColNamesVec.begin(),allColNamesVec.end(),*it);
            if(jt == allColNamesVec.end()) {
                cout << "Column " << *it << " does not exist" << endl;
                exit(0);
            }
            sortColIndexVec.push_back(jt - allColNamesVec.begin());
        }
        COL_ORDER = sortColIndexVec;
    }

    void sortFile() {
        sortBlocks();
        if(!tempFilesVec.empty())
            conquer_merge();
    }

    void sortBlocks() {
        long int lines_toread = (long int) maxMemSize/lineSize;

    }



};

int main(int argc,char ** argv) {
    if(argc<5) {
        cout << "Usage : main.cpp <input_file> <output_file> <memory_size> <asc/desc> <columns...>" << endl;
        return 0;
    }
    vector<string> sort_cols_vec;
    for(int i=5;i<argc;i++) {
        sort_cols_vec.push_back(argv[i]);
    }
    TwoPhaseMergeSort *kwms = new TwoPhaseMergeSort(argv[1],argv[2],argv[3],stoi(argv[4]),sort_cols_vec);
    kwms->sortFile();
    return 0;
}
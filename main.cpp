#include <iostream>
#include <fstream>
#include <sstream>
#include <sys/stat.h>
#include <vector>
#include <cstdio>
#include <algorithm>

using namespace std;

vector<int> COL_ORDER;

template <typename T>
class AscRecCompare {
public:
    int operator()(T const &r1, T const &r2) {
        for(int i=0;i<COL_ORDER.size();i++) {
            if(r1[i] == r2[i])
                continue;
            return (r1[i].compare(r2[i]));
        }
    }
};

template <typename T>
class DescRecCompare {
public:
    int operator()(T const &r1, T const &r2) {
        for(int i=0;i<COL_ORDER.size();i++) {
            if(r1[i] == r2[i])
                continue;
            return (-1*r1[i].compare(r2[i]));
        }
    }
};

class TwoPhaseMergeSort {
public:
    vector<int> sortColIndexVec;
    string inputFile;
    string outputFile;
    long int maxMemSize=0, recSize=0;
    string order;
    vector<string> sortColsVec;
    vector<pair<string,int>> metaVec;
    vector<string> tempFilesVec;
    vector<ifstream*> openTempFilesVec;
    vector<vector<string>> recordsVec;
    vector<string> tmpFilenamesVec;

    TwoPhaseMergeSort(string inFile, string outFile, int mem_size, string ord, vector<string> &sort_cols_vec) {
        inputFile = inFile;
        outputFile = outFile;
        maxMemSize = (long int) mem_size*1000000*0.8;
        sortColsVec = sort_cols_vec;
        order = "";
        for(int i=0;i<ord.length();i++)
            order += tolower(ord[i]);
        if(!(order=="asc" || order=="desc")) {
            cout << "Invalid order" << endl;
            exit(0);
        }
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
            metaVec.push_back(make_pair(colname,colsize));
            recSize += colsize;
        }
        metafile.close();
        for(auto it=sortColsVec.begin();it!=sortColsVec.end();it++) {
            bool flag = false;
            for(int j=0;j<metaVec.size();j++) {
                if(*it == metaVec[j].first) {
                    sortColIndexVec.push_back(j);
                    flag = true;
                    break;
                }
                if(!flag) {
                    cout << "Column " << *it << " does not exist" << endl;
                    exit(0);
                }
            }
//            auto jt = find(metaVec.begin(),metaVec.end(),*it);
//            if(jt == metaVec.end()) {
//                cout << "Column " << *it << " does not exist" << endl;
//                exit(0);
//            }
//            sortColIndexVec.push_back(jt - allColNamesVec.begin());
        }
        COL_ORDER = sortColIndexVec;
    }

    void sortFile() {
        cout << "Running Phase 1" << endl;
        phaseOneSort();
//        if(!tempFilesVec.empty())
//            conquer_merge();
    }

    void phaseOneSort() {
        long int recs_toread = (long int) maxMemSize/recSize;
        ifstream datafile(inputFile);
        string line;
        long int cur_line_count=0, all_recs_count=0;
        int sublist_num=1;
        while(cur_line_count < recs_toread && getline(datafile, line)) {
            vector<string> record;
            int start=0;
            for(int i=0;i<metaVec.size();i++) {
                record.push_back(line.substr(start,metaVec[i].second));
                start += metaVec[i].second + 2;
            }
            recordsVec.push_back(record);
            cur_line_count++;
            if(cur_line_count == recs_toread) {
                cout << "Sorting sublist #" << sublist_num << endl;
                if(order == "asc")
                    sort(recordsVec.begin(),recordsVec.end(),AscRecCompare<vector<string>>());
                else
                    sort(recordsVec.begin(),recordsVec.end(),DescRecCompare<vector<string>>());
                writeTempFile(sublist_num);
                cur_line_count = 0;
                sublist_num++;
                recordsVec.clear();
            }
            all_recs_count++;
        }
        if(recordsVec.size()>0) {
            cout << "Sorting sublist #" << sublist_num << endl;
            if(order == "asc")
                sort(recordsVec.begin(),recordsVec.end(),AscRecCompare<vector<string>>());
            else
                sort(recordsVec.begin(),recordsVec.end(),DescRecCompare<vector<string>>());
            writeTempFile(sublist_num);
            cur_line_count = 0;
            sublist_num++;
            recordsVec.clear();
            all_recs_count++;
        }
        cout << "# of records sorted : " << all_recs_count << endl;
    }

    void writeTempFile(int sublist_num) {
        cout << "Writing to disk #" << sublist_num << endl;
        string tmp_filename = "sublist."+to_string(sublist_num)+".tmp";
        ofstream *tmpfile;
        tmpfile = new ofstream(tmp_filename.c_str(), ios::out);
        for(int i=0; i<recordsVec.size(); i++) {
            string recline="";
            for(int j=0;j<recordsVec[i].size();j++) {
                if(j == recordsVec[i].size()-1)
                    recline += recordsVec[i][j]+"\n";
                else
                    recline += recordsVec[i][j]+"  ";
            }
            *tmpfile << recline;
        }
        tmpfile->close();
        delete tmpfile;
        tmpFilenamesVec.push_back(tmp_filename);
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
    TwoPhaseMergeSort *kwms = new TwoPhaseMergeSort(argv[1],argv[2],stoi(argv[3]),argv[4],sort_cols_vec);
    cout << "Started Execution" << endl;
    kwms->sortFile();
    return 0;
}
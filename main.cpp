#include <iostream>
#include <fstream>
#include <sstream>
#include <sys/stat.h>
#include <vector>
#include <cstdio>
#include <algorithm>
#include <bits/stdc++.h>

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
    vector<ifstream*> openTempFilesVec;
    vector<vector<string>> recordsVec;
    vector<string> tmpFilenamesVec;
    priority_queue<vector<string>,vector<vector<string>>,DescRecCompare<vector<string>>> PQ;

    TwoPhaseMergeSort(string inFile, string outFile, int mem_size, string ord, vector<string> &sort_cols_vec) {
        inputFile = inFile;
        outputFile = outFile;
        maxMemSize = (long int) mem_size*1000000*0.8;
        cout << "Memory size taken : " << maxMemSize << endl;
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
        cout << "Record size (without delimiter) : " << recSize << endl;
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
        }
        COL_ORDER = sortColIndexVec;
    }

    void sortFile() {
        cout << "Running Phase 1" << endl;
        phaseOneSort();
        if(!tmpFilenamesVec.empty()) {
            cout << "Running Phase 2" << endl;
            phaseTwoSort();
        }
    }

    void phaseOneSort() {
        struct stat filestatus;
        stat(inputFile.c_str(), &filestatus);
        long int filesize = filestatus.st_size;
        cout << "Number of sub-files (splits) : " << (long int)filesize/maxMemSize/recSize << endl;
        long int recs_toread = (long int) maxMemSize/recSize;
        cout << "Records to read per sub-file : " << recs_toread << endl;
        ifstream datafile(inputFile);
        string line;
        long int cur_line_count=0, all_recs_count=0;
        int sublist_num=1;
        while(cur_line_count < recs_toread && getline(datafile, line)) {
            recordsVec.push_back(recToVec(line));
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
            string recline = vecToStr(recordsVec[i], false);
            if(i == recordsVec.size()-1)
                *tmpfile << recline;
            else
                *tmpfile << recline << "\n";
        }
        tmpfile->close();
        delete tmpfile;
        tmpFilenamesVec.push_back(tmp_filename);
    }

    void phaseTwoSort() {
        openTempFiles();
        ofstream *outFile = new ofstream(outputFile.c_str(), ios::out);
        int sublist_num = tmpFilenamesVec.size();
        int block_size = (int)maxMemSize/(sublist_num*recSize);
        string line;
        bool completed_arr[sublist_num];
        int block_access_arr[sublist_num];
        for(int i=0;i<sublist_num;i++) {
            block_access_arr[i] = 0;
            completed_arr[i] = readDataBlock(i,block_size);
        }
        cout << "Sorting ..." << endl;
        while(PQ.empty() == false) {
            int last_col_idx = recordsVec[0].size()-1;
            vector<string> top_record = PQ.top();
            int top_block_num = stoi(top_record[last_col_idx]);
            block_access_arr[top_block_num]++;
            if(PQ.size()==1) {
                *outFile << vecToStr(top_record, true);
            }
            else {
                *outFile << vecToStr(top_record, true) << "\n";
            }
            PQ.pop();
            if(block_access_arr[top_block_num] >= block_size && !completed_arr[top_block_num]) {
                completed_arr[top_block_num] = readDataBlock(top_block_num,block_size);
            }
        }
        closeTempFiles();
        outFile->close();
    }

    vector<string> recToVec(string line) {
        int start=0;
        vector<string> record;
        for(int i=0;i<metaVec.size();i++) {
            record.push_back(line.substr(start,metaVec[i].second));
            start += metaVec[i].second + 2;
        }
        return record;
    }

    string vecToStr(vector<string> &rec, bool cutlast) {
        int n = cutlast ? rec.size()-1 : rec.size();
        string line="";
        for(int i=0;i<n;i++) {
            if(i == n-1)
                line += rec[i];
            else
                line += rec[i]+"  ";
        }
        return line;
    }

    bool readDataBlock(int filenum, int block_size) {
        string line;
        bool is_completed = false;
        for(int b=0;b<block_size;b++) {
            if(*openTempFilesVec[filenum] >> line) {
                vector<string> record = recToVec(line);
                record.push_back(to_string(filenum));
                recordsVec.push_back(record);
                if(b==0)
                    PQ.push(record);
            }
            else {
                is_completed = true;
                break;
            }
        }
        return is_completed;
    }

    void openTempFiles() {
        for (int i=0;i<tmpFilenamesVec.size();i++) {
            ifstream *file;
            file = new ifstream(tmpFilenamesVec[i].c_str(), ios::in);
            openTempFilesVec.push_back(file);
        }
    }

    void closeTempFiles() {
        for (int i=0;i<openTempFilesVec.size();i++) {
            openTempFilesVec[i]->close();
            remove(tmpFilenamesVec[i].c_str());
            delete openTempFilesVec[i];
        }
    }

};

int main(int argc, char ** argv) {
    if(argc<5) {
        cout << "Usage : main.cpp <input_file> <output_file> <memory_size> <asc/desc> <columns...>" << endl;
        return 0;
    }
    vector<string> sort_cols_vec;
    for(int i=5;i<argc;i++) {
        sort_cols_vec.push_back(argv[i]);
    }
    TwoPhaseMergeSort *tpms = new TwoPhaseMergeSort(argv[1],argv[2],stoi(argv[3]),argv[4],sort_cols_vec);
    cout << "Started Execution" << endl;
    tpms->sortFile();
    return 0;
}
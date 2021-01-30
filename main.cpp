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

bool ascSort(vector<string> const &r1, vector<string> const &r2) {
    for(int i=0;i<COL_ORDER.size();i++) {
        int c = COL_ORDER[i];
        if(r1[c] == r2[c])
            continue;
        return (r1[c].compare(r2[c]))<0;
    }
}

//class AscRecCompare {
//public:
//    bool operator()(vector<string> const &r1, vector<string> const &r2) {
//        for(int i=0;i<COL_ORDER.size();i++) {
//            int c = COL_ORDER[i];
//            if(r1[c] == r2[c])
//                continue;
//            return (r2[c].compare(r1[c]));
//        }
//    }
//};

bool descSort(vector<string> const &r1, vector<string> const &r2) {
    for(int i=0;i<COL_ORDER.size();i++) {
        int c = COL_ORDER[i];
        if(r1[c] == r2[c])
            continue;
        return (r1[c].compare(r2[c]))>0;
    }
}

//class DescRecCompare {
//public:
//    bool operator()(vector<string> const &r1, vector<string> const &r2) {
//        for(int i=0;i<COL_ORDER.size();i++) {
//            int c = COL_ORDER[i];
//            if(r1[c] == r2[c])
//                continue;
//            return (r1[c].compare(r2[c]))>0;
//        }
//    }
//};

struct CompareRecord {
    string order;
    CompareRecord(string order): order(order) {}
    bool operator() (vector<string> const &r1, vector<string> const &r2) const {
        if(order=="asc")
            return ascSort(r1,r2);
        return descSort(r1,r2);
    }
};

template <typename T>
class myPQ {
    public:
        priority_queue<vector<string>,vector<vector<string>>,T> PQ;
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
    bool *completed_arr;
    int lineSize;
//    priority_queue<vector<string>,vector<vector<string>>,T> PQ<T>;

    TwoPhaseMergeSort(string inFile, string outFile, int mem_size, string ord, vector<string> &sort_cols_vec) {
        inputFile = inFile;
        outputFile = outFile;
//        maxMemSize = (long int) mem_size*1000000*0.8;
        maxMemSize = (long int) mem_size*0.8;
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
//                cout << metaVec[j].first << " ";
                if(*it == metaVec[j].first) {
                    sortColIndexVec.push_back(j);
                    flag = true;
                    break;
                }
            }
            if(!flag) {
                cout << "Column " << *it << " does not exist" << endl;
                exit(0);
            }
        }
        COL_ORDER = sortColIndexVec;
        cout << "colorder";
        for(int a=0;a<COL_ORDER.size();a++)
            cout << COL_ORDER[a] << " ";
        cout << endl;
    }

    void sortFile() {
        cout << "Running Phase 1" << endl;
        phaseOneSort();
//        exit(0);
        if(!tmpFilenamesVec.empty()) {
            cout << "Running Phase 2" << endl;
            phaseTwoSort();
        }
    }

    void phaseOneSort() {
        struct stat filestatus;
        stat(inputFile.c_str(), &filestatus);
        long int filesize = filestatus.st_size;
        cout << "Filesize : " << filesize << endl;
        long int recs_toread = (long int) maxMemSize/recSize;
        cout << "Number of sub-files (splits) : " << (long int)filesize/(recs_toread*recSize) << endl;
        cout << "Records to read per sub-file : " << recs_toread << endl;
        ifstream datafile(inputFile);
        string line;
        long int cur_line_count=0, all_recs_count=0;
        int sublist_num=1;
        while(cur_line_count < recs_toread && getline(datafile, line)) {
            lineSize = line.length();
            recordsVec.push_back(recToVec(line));
            cur_line_count++;
            if(cur_line_count == recs_toread) {
                cout << "Sorting sublist #" << sublist_num << endl;
                if(order == "asc")
                    sort(recordsVec.begin(),recordsVec.end(),ascSort);
                else
                    sort(recordsVec.begin(),recordsVec.end(),descSort);
                writeTempFile(sublist_num);
//                exit(0);
                cur_line_count = 0;
                sublist_num++;
                recordsVec.clear();
            }
            all_recs_count++;
        }
        if(recordsVec.size()>0) {
            cout << "Sorting sublist #" << sublist_num << endl;
            if(order == "asc")
                sort(recordsVec.begin(),recordsVec.end(),ascSort);
            else
                sort(recordsVec.begin(),recordsVec.end(),descSort);
            writeTempFile(sublist_num);
            cur_line_count = 0;
            sublist_num++;
            recordsVec.clear();
        }
        cout << "Records sorted : " << all_recs_count << endl;
    }

    void writeTempFile(int sublist_num) {
        cout << "Writing to disk #" << sublist_num << endl;
        string tmp_filename = "sublist."+to_string(sublist_num)+".txt";
        ofstream tmpfile;
        tmpfile.open(tmp_filename.c_str(), ios_base::app);
        if(!tmpfile.is_open()) {
            cout << "not open " << tmp_filename << endl;
        }
        cout << "is open " << tmp_filename << " " << recordsVec.size() << endl;
        for(int i=0; i<recordsVec.size(); i++) {
            string recline = vecToStr(recordsVec[i], false);
//            cout << recline << endl;
                if (i == recordsVec.size() - 1)
                    tmpfile << recline;
                else
                    tmpfile << recline << "\n";
        }
        tmpfile.close();
        tmpFilenamesVec.push_back(tmp_filename);
    }

    void phaseTwoSort() {
        openTempFiles();
        ofstream *outFile = new ofstream(outputFile.c_str(), ios::out | ios::app);
        int sublist_num = tmpFilenamesVec.size();
        cout << "no of files " << sublist_num << endl;
        int block_size = (int)maxMemSize/(sublist_num*recSize);
        cout << "block size " << block_size << endl;
        string line;
        bool is_completed[sublist_num];
        completed_arr = is_completed;
        int block_access_arr[sublist_num];
        CompareRecord cmp(order);
        priority_queue<vector<string>,vector<vector<string>>,CompareRecord> PQ(order);
        for(int i=0;i<sublist_num;i++) {
            block_access_arr[i] = 0;
            PQ.push(readDataBlock(i,block_size));
//            cout << "hi" << endl;
        }
        cout << "Sorting ..." << endl;
        while(PQ.empty() == false) {
            int last_col_idx = recordsVec[0].size()-1;
            vector<string> top_record = PQ.top();
            int top_block_num = stoi(top_record[last_col_idx]);
            cout << "TOP " << top_block_num << " " << top_record[2] << endl;
            block_access_arr[top_block_num]++;
            if(PQ.size()==1) {
                *outFile << vecToStr(top_record, true) << "\n";
            }
            else {
                *outFile << vecToStr(top_record, true) << "\n";
            }
            cout << "before pop " << PQ.size();
            PQ.pop();
            cout << " after pop " << PQ.size() << endl;
            if(block_access_arr[top_block_num] >= block_size && !completed_arr[top_block_num]) {
                PQ.push(readDataBlock(top_block_num,block_size));
                cout << "here" << PQ.size() << endl;
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

    vector<string> readDataBlock(int filenum, int block_size) {
        string line, word;
        vector<string> rec0;
        char cline[lineSize+1];
//        bool is_completed = false;
        for(int b=0;b<block_size;b++) {
            cout << "before getline" << endl;
//            if(getline(openTempFilesVec[filenum],line)) {
//            if(fgets(cline,lineSize+1,openTempFilesVec[filenum])) {
            if(*openTempFilesVec[filenum] >> word) {
//                line = string(cline);
                line += word+"  ";
                cout << "read " << line << endl;
                for(int w=1;w<metaVec.size();w++){
                    *openTempFilesVec[filenum] >> word;
                    line += "  "+word;
                }
                vector<string> record = recToVec(line);
                record.push_back(to_string(filenum));
                recordsVec.push_back(record);
                if(b==0)
                    rec0 = record;
            }
            else {
                completed_arr[filenum] = true;
                break;
            }
            cout << "read " << filenum << endl;
        }
        return rec0;
    }

    void openTempFiles() {
        for (int i=0;i<tmpFilenamesVec.size();i++) {
            ifstream *tmpfile = new ifstream(tmpFilenamesVec[i].c_str(), ios::in);;
//            tmpfile.open(tmpFilenamesVec[i].c_str(), ios::in);
            openTempFilesVec.push_back(tmpfile);
//            FILE *tmpfile = fopen(tmpFilenamesVec[i].c_str(), "r");
//            openTempFilesVec.push_back(tmpfile);
//            tmpfile.open(tmpFilenamesVec[i].c_str(), ios::in);

        }
    }

    void closeTempFiles() {
        for (int i=0;i<openTempFilesVec.size();i++) {
            openTempFilesVec[i]->close();
//            fclose(openTempFilesVec[i]);
//            remove(tmpFilenamesVec[i].c_str());
//            delete openTempFilesVec[i];
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
//        cout << argv[i] << ", ";
    }
//    cout << endl;
    TwoPhaseMergeSort *tpms = new TwoPhaseMergeSort(argv[1],argv[2],stoi(argv[3]),argv[4],sort_cols_vec);
    cout << "Started Execution" << endl;
    tpms->sortFile();
    return 0;
}
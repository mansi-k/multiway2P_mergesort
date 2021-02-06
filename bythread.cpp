#include <iostream>
#include <sys/stat.h>
#include <vector>
#include <cstdio>
#include <algorithm>
#include <bits/stdc++.h>
#include <pthread.h>
#include <chrono>

using namespace std;
using namespace std::chrono;

#define THREADLIMIT 10

vector<int> COL_ORDER;
pthread_t TIDarr[THREADLIMIT] = {0};

bool ascSort(vector<string> const &r1, vector<string> const &r2) {
    for(int i=0;i<COL_ORDER.size();i++) {
        int c = COL_ORDER[i];
        if(r1[c] == r2[c])
            continue;
        return (r1[c].compare(r2[c]))<0;
    }
}

class AscRecCompare {
public:
    bool operator()(vector<string> const &r1, vector<string> const &r2) {
        for(int i=0;i<COL_ORDER.size();i++) {
            int c = COL_ORDER[i];
            if(r1[c] == r2[c])
                continue;
            return (r2[c].compare(r1[c]));
        }
    }
};

bool descSort(vector<string> const &r1, vector<string> const &r2) {
    for(int i=0;i<COL_ORDER.size();i++) {
        int c = COL_ORDER[i];
        if(r1[c] == r2[c])
            continue;
        return (r1[c].compare(r2[c]))>0;
    }
}

class DescRecCompare {
public:
    bool operator()(vector<string> const &r1, vector<string> const &r2) {
        for(int i=0;i<COL_ORDER.size();i++) {
            int c = COL_ORDER[i];
            if(r1[c] == r2[c])
                continue;
            return (r1[c].compare(r2[c]))>0;
        }
    }
};


struct CompareRecord {
    string order;
    CompareRecord(string order): order(order) {}
    bool operator() (vector<string> const &r1, vector<string> const &r2) const {
        if(order=="asc")
            return descSort(r1,r2);
        return ascSort(r1,r2);
    }
};


vector<ifstream*> openTempFilesVec;
long int lineSize, lines_per_thread, total_lines, lines_fit;
vector<vector<string>> recordsVec;
vector<pair<string,int>> metaVec;
string inputFile;
vector<int> sortColIndexVec;
string outputFile;
long int maxMemSize=0, recSize=0;
string order;
vector<string> sortColsVec;
vector<string> tmpFilenamesVec;
bool *completed_arr;
vector<int> num_recs_per_file_vec;
int num_threads=0;

//    priority_queue<vector<string>,vector<vector<string>>,T> PQ<T>;
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
    cout << "Record size in bytes (without delimiter) : " << recSize << endl;
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
//    cout << "colorder";
//    for(int a=0;a<COL_ORDER.size();a++)
//        cout << COL_ORDER[a] << " ";
//    cout << endl;
}

void TwoPhaseMergeSort(string inFile, string outFile, int mem_size, int nthreads, string ord, vector<string> &sort_cols_vec) {
    inputFile = inFile;
    outputFile = outFile;
    maxMemSize = (long int) mem_size*1000000;
//    maxMemSize = (long int) mem_size;
    cout << "Memory size taken (in bytes) : " << maxMemSize << endl;
    num_threads = nthreads;
    if(num_threads > THREADLIMIT) {
        cout << "Maximum no. of threads allowed is " << THREADLIMIT << endl;
        exit(0);
    }
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

vector<string> recToVec(string line) {
    int start=0;
    vector<string> record;
    for(int i=0;i<metaVec.size();i++) {
        record.push_back(line.substr(start,metaVec[i].second));
        start += metaVec[i].second + 2;
    }
    return record;
}

void* readInputFile(void *args) {
//    cout << "here" << endl;
    pair<long int, long int> clbp = *((pair<long int, long int> *)args);
    long int start_line = clbp.first;
    long int block_num = clbp.second;
    ifstream datafile(inputFile);
    datafile.seekg(start_line*lineSize, ios::beg);
//    cout << "Seeking to " << start_line << " in bytes :" << start_line*lineSize << endl;
//    int x;
//    cin >> x;
    long int cur_lineno = start_line;
    long int end_lineno = start_line+lines_per_thread;
    long int block_lines = lines_fit*(block_num+1);
    string line="";
    while(cur_lineno<total_lines && cur_lineno<block_lines && cur_lineno<end_lineno && getline(datafile, line)) {
//        cout << line << endl;
        recordsVec.push_back(recToVec(line));
        cur_lineno++;
    }
    datafile.close();
//    cout << "returning" << endl;
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

void writeTempFile(int sublist_num) {
    cout << "Writing to disk #" << sublist_num << endl;
    string tmp_filename = "sublist."+to_string(sublist_num)+".txt";
    ofstream tmpfile;
    tmpfile.open(tmp_filename.c_str(), ios_base::app);
    if(!tmpfile.is_open()) {
        cout << "File cannot open " << tmp_filename << endl;
    }
//        cout << "is open " << tmp_filename << " " << recordsVec.size() << endl;
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
    num_recs_per_file_vec.push_back(recordsVec.size());
}

void phaseOneSort() {
    struct stat filestatus;
    stat(inputFile.c_str(), &filestatus);
    long int filesize = filestatus.st_size;
    ifstream datafile(inputFile);
    string line;
    getline(datafile, line);
    lineSize = line.length()+1;
    cout << "Size of a line (in bytes) : " << lineSize << endl;
    total_lines = filesize/lineSize;
    lines_fit = (long int) maxMemSize/lineSize;
    datafile.close();
//    cout << "No of sub-lists : " << total_lines/lines_fit << endl;
    lines_per_thread = ceil((double)lines_fit/num_threads);
    cout << "Lines per thread : " << lines_per_thread << endl;
    long int lines_read=0, block_num=0;
    while(lines_read < total_lines) {
        int long block_lines = lines_fit*(block_num+1);
        for (long int t=0,start_line = lines_read; t<num_threads && lines_read<total_lines && start_line<block_lines; t++,start_line=lines_read) {
            pair<long int, long int> clbp = make_pair(start_line,block_num);
            cout << "Creating thread #" << t << endl;
            if (pthread_create(&TIDarr[t], NULL, readInputFile, &clbp) != 0) {
                perror("\nFailed to create thread ");
            }
//            cout << "joining" << t << endl;
            pthread_join(TIDarr[t],NULL);
            lines_read = min(lines_read+lines_per_thread,lines_read+lines_fit);
//            cout << "lines_read" << lines_read << endl;
//            cout << recordsVec.size() << endl;
//            int x;
//            cin >> x;
        }
        if(!recordsVec.empty()) {
            cout << "Sorting sublist #" << block_num << endl;
            if (order == "asc")
                sort(recordsVec.begin(), recordsVec.end(), ascSort);
            else
                sort(recordsVec.begin(), recordsVec.end(), descSort);
            writeTempFile(block_num);
            recordsVec.clear();
            block_num++;
        }
    }
//    cout << "Records sorted : " << lines_read << endl;
}

void openTempFiles() {
    for (int i=0;i<tmpFilenamesVec.size();i++) {
        openTempFilesVec.push_back(new ifstream(tmpFilenamesVec[i].c_str(), ios::in));
    }
}

void closeTempFiles() {
    for (int i=0;i<openTempFilesVec.size();i++) {
        openTempFilesVec[i]->close();
        remove(tmpFilenamesVec[i].c_str());
    }
}

vector<vector<string>> readDataBlock(int filenum, int block_size) {
    string line, word;
    vector<vector<string>> block_recs;
    for(int b=0;b<block_size;b++) {
//            cout << "before getline " << filenum << endl;
        if(openTempFilesVec[filenum]->peek() && getline(*openTempFilesVec[filenum],line)) {
//                cout << "read " << line << endl;
            vector<string> record = recToVec(line);
            record.push_back(to_string(filenum));
            recordsVec.push_back(record);
            block_recs.push_back(record);
            num_recs_per_file_vec[filenum]--;
            if(num_recs_per_file_vec[filenum]<=0) {
                completed_arr[filenum] = true;
                break;
            }
        }
        else {
//                completed_arr[filenum] = true;
            cout << "error reading " << filenum << endl;
            break;
        }
//            cout << "read " << filenum << endl;
    }
    return block_recs;
}

void phaseTwoSort() {
    openTempFiles();
    ofstream *outFile = new ofstream(outputFile.c_str(), ios::out | ios::app);
    int sublist_num = tmpFilenamesVec.size();
    cout << "No of sub-files " << sublist_num << endl;
    int block_size = (int)maxMemSize/(sublist_num*recSize);
    cout << "Block size (no of records) : " << block_size << endl;
    if(block_size<=0) {
        cout << "Cannot sort as memory size is too small" << endl;
        exit(0);
    }
    string line;
    bool is_completed[sublist_num];
    completed_arr = is_completed;
    int block_access_arr[sublist_num];
    CompareRecord cmp(order);
    priority_queue<vector<string>,vector<vector<string>>,CompareRecord> PQ(order);
    for(int i=0;i<sublist_num;i++) {
        block_access_arr[i] = 0;
        for(vector<string> const rvec : readDataBlock(i,block_size))
            PQ.push(rvec);
//            cout << "hi" << endl;
    }
    cout << "Sorting ..." << endl;
    while(PQ.empty() == false) {
        int last_col_idx = recordsVec[0].size()-1;
        vector<string> top_record = PQ.top();
        int top_block_num = stoi(top_record[last_col_idx]);
//        cout << "TOP " << top_block_num << " " << top_record[2] << endl;
        block_access_arr[top_block_num]++;
//        cout << top_block_num << "access" << block_access_arr[top_block_num] << endl;
        if(PQ.size()==1) {
            *outFile << vecToStr(top_record, true) << "\n";
        }
        else {
            *outFile << vecToStr(top_record, true) << "\n";
        }
//            cout << "before pop " << PQ.size();
        PQ.pop();
//            cout << " after pop " << PQ.size() << endl;
        if(block_access_arr[top_block_num] >= block_size && !completed_arr[top_block_num]) {
//                PQ.push(readDataBlock(top_block_num,block_size));
            for(vector<string> const rvec : readDataBlock(top_block_num,block_size))
                PQ.push(rvec);
            block_access_arr[top_block_num] = 0;
//                cout << "here" << PQ.size() << endl;
        }
    }
    cout << "Sorted all records" << endl;
    outFile->close();
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


int main(int argc, char ** argv) {
    if(argc<6) {
        cout << "Usage : main.cpp <input_file> <output_file> <memory_size> <num_threads> <asc/desc> <columns...>" << endl;
        return 0;
    }
    vector<string> sort_cols_vec;
    for(int i=6;i<argc;i++) {
        sort_cols_vec.push_back(argv[i]);
//        cout << argv[i] << ", ";
    }
//    cout << endl;
    TwoPhaseMergeSort(argv[1],argv[2],stoi(argv[3]),stoi(argv[4]),argv[5],sort_cols_vec);
    cout << "Started Execution" << endl;
    auto ex_start = high_resolution_clock::now();
    sortFile();
    auto ex_stop = high_resolution_clock::now();
    auto duration = duration_cast<milliseconds>(ex_stop - ex_start);
    cout << "Time taken for sorting : " << ((double)duration.count()/1000) << " seconds" << endl;
    closeTempFiles();
    return 0;
}
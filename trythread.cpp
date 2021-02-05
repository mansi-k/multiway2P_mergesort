#include <iostream>
#include <fstream>
#include <sstream>
#include <sys/stat.h>
#include <vector>
#include <cstdio>
#include <algorithm>
#include <bits/stdc++.h>
using namespace std;

int main() {
    ofstream tmpfile;
    tmpfile.open("manu.txt", ios_base::app);
    if(!tmpfile.is_open()) {
        cout << "not open " << endl;
    }
    else
        cout << "opnd" << endl;
    tmpfile.close();
    return 0;
}
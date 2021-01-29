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
    std::ofstream outfile;
string str = "^#%#%^";
  outfile.open("test.txt", std::ios_base::app); // append instead of overwrite
  outfile << str << "\n";
outfile << "bhjvghv"; 
  return 0;
}

/*
 * @Author: your name
 * @Date: 2020-09-04 15:09:54
 * @LastEditTime: 2020-09-04 15:18:35
 * @LastEditors: Please set LastEditors
 * @Description: In User Settings Edit
 * @FilePath: /Sherman/test/main.cpp
 */
#include <boost/version.hpp>
#include <iostream>

int main()
{
    std::cout << "Hello World!\n";
    std::cout << "Boost version: " << BOOST_VERSION / 100000 << "."
              << BOOST_VERSION / 100 % 1000 << "." << BOOST_VERSION % 100
              << std::endl;
    int(*A5[3])[5];
    int((*(A6[3]))[5]);
    using T1 = decltype(A5);
    using T2 = decltype(A6);
    bool same = std::is_same_v<T1, T2>;
    std::cout << "! same: " << same << std::endl;
    return 0;
}
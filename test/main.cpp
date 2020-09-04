/*
 * @Author: your name
 * @Date: 2020-09-04 15:09:54
 * @LastEditTime: 2020-09-04 15:18:35
 * @LastEditors: Please set LastEditors
 * @Description: In User Settings Edit
 * @FilePath: /Sherman/test/main.cpp
 */
#include <iostream>
#include <boost/version.hpp>

int main()
{
    std::cout << "Hello World!\n";
    std::cout << "Boost version: "
              << BOOST_VERSION / 100000
              << "."
              << BOOST_VERSION / 100 % 1000
              << "."
              << BOOST_VERSION % 100
              << std::endl;
    return 0;
}
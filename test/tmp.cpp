#include <algorithm>
#include <iostream>
#include <list>
void print(const std::list<int> &ls)
{
    for (auto i : ls)
    {
        std::cout << i << ", ";
    }
    std::cout << std::endl;
}
int main()
{
    std::list<int> ls{1, 2, 3, 4, 5};
    print(ls);

    auto it = std::find(ls.begin(), ls.end(), 3);
    std::cout << "find " << *it << std::endl;

    ls.splice(ls.begin(), ls, it);
    print(ls);

    return 0;
}
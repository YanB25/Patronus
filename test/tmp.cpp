#include "util/monitor.h"

class Son
{
public:
	Son() = default;
	~Son()
	{
		std::cerr << "Son" << std::endl;
	}
};
class Father
{
public:
    Father(const int& a): son_(std::make_shared<Son>()), a(a)
	{
	}
    ~Father()
    {
        std::cerr << "father" << std::endl;
    }
	Father& operator=(Father&&)
	{
		// a = std::move(rhs);
		return *this;
	}
	std::shared_ptr<Son> son_;
private:
	[[maybe_unused]] const int& a;
};
int main()
{
}
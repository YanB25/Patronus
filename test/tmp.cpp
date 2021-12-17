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
	uint64_t data = 0xaa00abcdef121212;
	char w_buf[2];
	std::string str;
	char* buf = (char*) &data;
	for (size_t i = 0; i < sizeof(data); ++i)
	{
		fprintf(stderr, "%x", buf[i] & 0xff);
		sprintf(w_buf, "%x", buf[i] & 0xff);
		fprintf(stderr, "%x", buf[i] >> 4);
		sprintf(w_buf, "%x", buf[i] >> 4);
		str += std::string(w_buf, 2);
	}
	fprintf(stderr, "It is %s\n", str.c_str());
	return 0;
}
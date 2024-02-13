
#include <iostream>
//#include <concepts>

class Foo {
public:
  //Foo operator+ (const Foo& other) {
  //  return Foo(); 
  //}
};

template<typename T> 
requires std::integral<T> || std::floating_point<T>
T sum(T a, T b)
{
    return a+b;
}

int main ()
{
    std::cout << sum(2,4)<< std::endl;
    std::cout << sum(2.2f,4.5f)<< std::endl;

    sum(Foo(), Foo());
    return 0;
}


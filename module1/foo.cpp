
module; //global module
#include <iostream>
export module foo;
import "training.h";

void print()
{
    std::cout << "Hello world!\n";
    auto v = life;

    std::cout << "life" << v << std::endl;

}


export namespace training {
        void hello()       // export declaration
        {
            print();

        }

        int ret()       // export declaration
        {
            return 0;        
        }
}

export 
{
    class hola_st
    {

    };

    class hola_class
    {

    };

}

export struct hola_st2
{
    
};


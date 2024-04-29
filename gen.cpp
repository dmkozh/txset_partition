#include <iostream>
#include <vector>
#include <random>
#include <variant>

using namespace std;

/*
TODO
1. Input Validation
2. Set seed
3. Limit range of fee and insns?
*/

struct Random
{
    // The number of transactions to generate
    int num_txs;

    // The range of random possible values in the footprint will be [0, entry_range]
    int entry_range;

    // read_only and read_write will each be randomly populated with between [1, footprint_entry_limit] entries
    int footprint_entry_limit;

    // Minimum percentage of num_txs that will have a write conflict on a single key. Value between [0, 1]
    double percent_of_txs_with_conflict;
};

struct Oracle
{
};

struct Config
{
    enum{RANDOM, ORACLE} tag;
    union
    {
        Random random;
        Oracle oracle;
    };
};

struct Tx {
  int tx_id = 0;
  int insns = 0;
  int fee = 0;
  vector<int> read_only;
  vector<int> read_write;
};

vector<Tx> 
oracle(Oracle const& config)
{
    return {};
}

vector<Tx> 
random(Random const& config)
{
    std::random_device rd;
    std::mt19937 gen(rd());

    std::uniform_int_distribution<> insFeeDist(1, INT32_MAX);
    std::uniform_int_distribution<> entryDist(0, config.entry_range);
    std::uniform_int_distribution<> footprintSizeDist(1, config.footprint_entry_limit);

    vector<Tx> res;
    for(size_t i = 0; i < config.num_txs; ++i)
    {   
        res.push_back({static_cast<int>(i), insFeeDist(gen), insFeeDist(gen), {}, {}});
        

        std::generate_n(std::back_inserter(res.back().read_only), footprintSizeDist(gen), [&]{ return entryDist(gen); });
        std::generate_n(std::back_inserter(res.back().read_write), footprintSizeDist(gen), [&]{ return entryDist(gen); });
    }

    
    int conflict_key = config.entry_range + 1;
    int numConflicts = config.percent_of_txs_with_conflict * config.num_txs;
    for(auto& tx : res)
    {
        if(numConflicts-- == 0)
        {
            break;
        }

        tx.read_write.emplace_back(conflict_key);
    }
    return res;
}

vector<Tx> 
generate(Config const& config)
{
    switch (config.tag)
    {
    case Config::RANDOM:
        return random(config.random);
        break;
    
    default:
        break;
    }
}

int main()
{
    Config c = {Config::RANDOM, {10, 10, 15, .5}};

    auto res = generate(c);
    
    for(auto a: res)
    {
        cout << a.tx_id << "  " << a.fee << "    " << a.insns << endl;

        cout << "read_only ";
        for (auto r : a.read_only)
            cout << r << " ";
        cout << "\n";

        cout << "read_write ";
        for (auto w : a.read_write)
            cout << w << " ";
        cout << "\n\n";
    }
}
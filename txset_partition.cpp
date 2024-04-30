#include <algorithm>
#include <chrono>
#include <iostream>
#include <random>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

using namespace std;
using namespace chrono;

struct Tx {
  int tx_id = 0;
  int insns = 0;
  int fee = 0;
  vector<int> read_only;
  vector<int> read_write;
  Tx() = default;
  Tx(int tx_id, int insns, int fee) : tx_id(tx_id), insns(insns), fee(fee) {}
};

struct Cluster {
  int insns = 0;
  unordered_set<int> read_only;
  unordered_set<int> read_write;
  vector<int> txs;

  Cluster() = default;

  explicit Cluster(const Tx& tx) : insns(tx.insns) {
    read_only.insert(tx.read_only.begin(), tx.read_only.end());
    read_write.insert(tx.read_write.begin(), tx.read_write.end());
    txs.push_back(tx.tx_id);
  }

  void merge(const Cluster& other) {
    insns += other.insns;
    read_only.insert(other.read_only.begin(), other.read_only.end());
    read_write.insert(other.read_write.begin(), other.read_write.end());
    txs.insert(txs.end(), other.txs.begin(), other.txs.end());
  }
};

struct TxSetStage {
  vector<vector<Tx>> txs;
};

struct TxSet {
  vector<TxSetStage> stages;
};

struct ParitionConfig {
  int stage_count = 0;
  int threads_per_stage = 0;
  int insns_per_thread = 0;

  int64_t insnsPerStage() const {
    return static_cast<int64_t>(threads_per_stage) * insns_per_thread;
  }

  int64_t maxInsns() const { return insnsPerStage() * stage_count; }
};

class Stage {
 public:
  Stage(ParitionConfig cfg) : cfg_(cfg) {}

  bool tryAdd(const Tx& tx) {
    if (total_insns_ + tx.insns > cfg_.insnsPerStage()) {
      return false;
    }

    auto conflicting_clusters = getConflictingClusters(tx);
    auto new_clusters = createNewClusters(tx, conflicting_clusters);
    auto packing = binPacking(new_clusters);
    if (packing.empty()) {
      return false;
    }
    clusters_ = new_clusters;
    packing_ = packing;
    total_insns_ += tx.insns;
    return true;
  }

  vector<vector<int>> getPerThreadTxs() const { return packing_; }

 private:
  unordered_set<const Cluster*> getConflictingClusters(const Tx& tx) const {
    unordered_set<const Cluster*> conflicting_clusters;
    for (const Cluster& cluster : clusters_) {
      bool is_conflicting = false;
      for (int ro : tx.read_only) {
        if (cluster.read_write.count(ro) > 0) {
          is_conflicting = true;
          break;
        }
      }
      for (int rw : tx.read_write) {
        if (cluster.read_only.count(rw) > 0 ||
            cluster.read_write.count(rw) > 0) {
          is_conflicting = true;
          break;
        }
      }
      if (is_conflicting) {
        conflicting_clusters.insert(&cluster);
      }
    }
    return conflicting_clusters;
  }

  vector<Cluster> createNewClusters(
      const Tx& tx,
      const unordered_set<const Cluster*>& conflicting_clusters) const {
    vector<Cluster> new_clusters;
    new_clusters.reserve(clusters_.size());
    for (const auto& cluster : clusters_) {
      if (conflicting_clusters.count(&cluster) == 0) {
        new_clusters.push_back(cluster);
      }
    }

    new_clusters.emplace_back(tx);
    for (const auto* cluster : conflicting_clusters) {
      new_clusters.back().merge(*cluster);
    }
    sort(new_clusters.begin(), new_clusters.end(),
         [](const auto& a, const auto& b) { return a.insns > b.insns; });
    return new_clusters;
  }

  vector<vector<int>> binPacking(const vector<Cluster>& clusters) const {
    const int bin_count = cfg_.threads_per_stage;
    vector<vector<int>> bins(bin_count);
    vector<int> bin_insns(bin_count);
    for (const auto& cluster : clusters) {
      bool packed = false;
      for (int i = 0; i < bin_count; ++i) {
        if (bin_insns[i] + cluster.insns <= cfg_.insns_per_thread) {
          bin_insns[i] += cluster.insns;
          bins[i].insert(bins[i].end(), cluster.txs.begin(), cluster.txs.end());
          packed = true;
          break;
        }
      }
      if (!packed) {
        return vector<vector<int>>();
      }
    }
    return bins;
  }

  vector<Cluster> clusters_;
  vector<vector<int>> packing_;
  int64_t total_insns_ = 0;
  ParitionConfig cfg_;
};

TxSet partition(vector<Tx>& txs, ParitionConfig cfg) {
  sort(txs.begin(), txs.end(),
       [](const Tx& a, const Tx& b) { return a.fee > b.fee; });
  vector<Stage> stages(cfg.stage_count, cfg);
  for (const auto& tx : txs) {
    for (auto& stage : stages) {
      if (stage.tryAdd(tx)) {
        break;
      }
    }
  }
  unordered_map<int, const Tx*> tx_map;
  for (const auto& tx : txs) {
    tx_map[tx.tx_id] = &tx;
  }
  TxSet res;
  for (const auto& stage : stages) {
    res.stages.emplace_back();
    auto& tx_set_stage = res.stages.back();
    auto stage_txs = stage.getPerThreadTxs();
    for (const auto& thread : stage_txs) {
      tx_set_stage.txs.emplace_back();
      auto& tx_set_thread = tx_set_stage.txs.back();
      for (int tx_id : thread) {
        tx_set_thread.push_back(*tx_map[tx_id]);
        tx_map.erase(tx_id);
      }
    }
  }
  vector<Tx> new_txs;
  for (const auto& tx : txs) {
    if (tx_map.count(tx.tx_id) > 0) {
      new_txs.push_back(tx);
    }
  }
  txs = new_txs;
  return res;
}

struct Random {
  int64_t total_insns;
  int max_footprint_entries;
  int max_footprint_rw_entries;
  double ro_to_rw_conflict_prob;
  double rw_to_rw_conflict_prob;
};

struct Oracle {};

struct Config {
  enum { RANDOM, ORACLE } tag;
  union {
    Random random;
    Oracle oracle;
  };
};

vector<Tx> oracle(Oracle const& config) { return {}; }

vector<Tx> random(Random const& config, int64_t& generatedInsns) {
  mt19937 gen(123);

  normal_distribution<> insDist(20'000'000, 15'000'000);

  uniform_int_distribution<> feeDist(100, 1'000'000);
  uniform_int_distribution<> footprintSizeDist(2, config.max_footprint_entries);

  vector<Tx> res;
  int entryId = 0;
  generatedInsns = 0;
  int txId = 0;
  while (generatedInsns < config.total_insns) {
    int insns = max(500'000, min(100'000'000, static_cast<int>(insDist(gen))));
    res.emplace_back(txId++, insns, feeDist(gen));
    auto& tx = res.back();
    generatedInsns += tx.insns;
    int footprintSize = footprintSizeDist(gen);
    uniform_int_distribution<> rwCountDistr(
        1, min(footprintSize, config.max_footprint_rw_entries));
    int rwCount = rwCountDistr(gen);
    int roCount = footprintSize - rwCount;
    for (int i = 0; i < roCount; ++i) {
      tx.read_only.push_back(entryId++);
    }
    for (int i = 0; i < rwCount; ++i) {
      tx.read_write.push_back(entryId++);
    }
  }

  uniform_real_distribution<> probDistr;
  for (int i = 0; i < res.size(); ++i) {
    for (int j = i + 1; j < res.size(); ++j) {
      double p = probDistr(gen);
      uniform_int_distribution<> fpDistr(0, res[i].read_write.size() - 1);

      if (p < config.ro_to_rw_conflict_prob) {
        res[j].read_only.push_back(res[i].read_write[fpDistr(gen)]);
      } else if (p < config.ro_to_rw_conflict_prob +
                         config.rw_to_rw_conflict_prob) {
        res[j].read_write.push_back(res[i].read_write[fpDistr(gen)]);
      }
    }
  }

  return res;
}

vector<Tx> generate(Config const& config, int64_t& generatedInsns) {
  switch (config.tag) {
    case Config::RANDOM:
      return random(config.random, generatedInsns);
      break;

    default:
      break;
  }
}

Config randomTxsConfig(int64_t max_insns, double ro_to_rw_conflict_prob,
                       double rw_to_rw_conflict_prob) {
  Config cfg;
  cfg.tag = Config::RANDOM;
  cfg.random.max_footprint_entries = 65;
  cfg.random.max_footprint_rw_entries = 25;
  cfg.random.total_insns = max_insns;
  cfg.random.ro_to_rw_conflict_prob = ro_to_rw_conflict_prob;
  cfg.random.rw_to_rw_conflict_prob = rw_to_rw_conflict_prob;
  return cfg;
}

int main() {
  ParitionConfig cfg;
  cfg.stage_count = 4;
  cfg.threads_per_stage = 8;
  cfg.insns_per_thread = 125'000'000;

  auto genConfig = randomTxsConfig(cfg.maxInsns() * 2, 0.05, 0.05);

  int64_t generated_insns = 0;
  auto txs = generate(genConfig, generated_insns);
  cout << "Tx count: " << txs.size() << endl;
  int iter = 0;
  while (!txs.empty()) {
    auto start = high_resolution_clock::now();
    auto txSet = partition(txs, cfg);
    auto stop = high_resolution_clock::now();
    auto duration = duration_cast<milliseconds>(stop - start);
    cout << "Partition time: " << duration.count() << " ms" << endl;
    int64_t insns_left = 0;
    for (const auto& tx : txs) {
      insns_left += tx.insns;
    }
    cout << "Init percent left: " << 100.0 * insns_left / generated_insns
         << endl;
    ++iter;
  }
  cout << "Total iter: " << iter << endl;
}

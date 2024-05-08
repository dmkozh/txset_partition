#include <algorithm>
#include <chrono>
#include <fstream>
#include <iostream>
#include <optional>
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

  void validate() {
    for (const auto& stage : stages) {
      unordered_set<int> ro;
      unordered_set<int> rw;
      for (const auto& thread : stage.txs) {
        for (const auto& tx : thread) {
          for (int ro_entry : tx.read_only) {
            if (rw.count(ro_entry) > 0) {
              throw runtime_error("RO conflict");
            }
          }
          for (int rw_entry : tx.read_write) {
            if (ro.count(rw_entry) > 0 || rw.count(rw_entry) > 0) {
              throw runtime_error("RW conflict");
            }
          }
        }
        for (const auto& tx : thread) {
          ro.insert(tx.read_only.begin(), tx.read_only.end());
          rw.insert(tx.read_write.begin(), tx.read_write.end());
        }
      }
    }
  }
};

struct PartitionConfig {
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
  Stage(PartitionConfig cfg) : cfg_(cfg) {}

  optional<int> tryAdd(const Tx& tx, bool do_add) {
    if (total_insns_ + tx.insns > cfg_.insnsPerStage()) {
      return nullopt;
    }

    auto conflicting_clusters = getConflictingClusters(tx);
    int conflicting_insns = 0;
    for (const auto* c : conflicting_clusters) {
      conflicting_insns += c->insns;
    }
    auto new_clusters = createNewClusters(tx, conflicting_clusters);
    vector<int> new_bin_insns;
    auto packing = binPacking(new_clusters, new_bin_insns);
    if (packing.empty()) {
      return nullopt;
    }
    if (do_add) {
      clusters_ = new_clusters;
      packing_ = packing;
      total_insns_ += tx.insns;
      bin_insns_ = new_bin_insns;
    }
    return conflicting_insns;
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
          // if (cluster.read_write.count(rw) > 0) {
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

  vector<vector<int>> binPacking(const vector<Cluster>& clusters,
                                 vector<int>& bin_insns) const {
    const int bin_count = cfg_.threads_per_stage;
    vector<vector<int>> bins(bin_count);
    bin_insns.resize(bin_count);
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
  vector<int> bin_insns_;
  int64_t total_insns_ = 0;
  PartitionConfig cfg_;
};

TxSet partition(vector<Tx>& txs, PartitionConfig cfg) {
  sort(txs.begin(), txs.end(),
       [](const Tx& a, const Tx& b) { return a.fee > b.fee; });
  // sort(txs.begin(), txs.end(),
  //      [](const Tx& a, const Tx& b) { return a.insns > b.insns; });
  vector<Stage> stages(cfg.stage_count, cfg);

  for (const auto& tx : txs) {
    int min_conflicting_insns = numeric_limits<int>::max();
    Stage* best_stage = nullptr;
    for (auto& stage : stages) {
      auto maybe_conflicting_insns = stage.tryAdd(tx, true);
      if (maybe_conflicting_insns &&
          *maybe_conflicting_insns < min_conflicting_insns) {
        min_conflicting_insns = *maybe_conflicting_insns;
        best_stage = &stage;
        break;
      }
    }
    // if (best_stage != nullptr) {
    //   best_stage->tryAdd(tx, true);
    // }
  }
  unordered_map<int, const Tx*> tx_map;
  for (const auto& tx : txs) {
    tx_map[tx.tx_id] = &tx;
  }
  TxSet res;
  for (const auto& stage : stages) {
    auto& tx_set_stage = res.stages.emplace_back();
    auto stage_txs = stage.getPerThreadTxs();
    for (const auto& thread : stage_txs) {
      auto& tx_set_thread = tx_set_stage.txs.emplace_back();
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

const int64_t INSNS_PER_THREAD = 500'000'000;
const int64_t THREADS = 8;
const int64_t INSNS_PER_LEDGER = 2 * INSNS_PER_THREAD * THREADS;

struct Conflict {
  double fraction_of_txs_ro;
  double fraction_of_txs_rw;

  Conflict(double fraction_of_txs_ro, double fraction_of_txs_rw)
      : fraction_of_txs_ro(fraction_of_txs_ro),
        fraction_of_txs_rw(fraction_of_txs_rw) {}
};

struct PredefinedConflictsConfig {
  int64_t total_insns;
  double conflicts_per_tx;
  double mean_ro_entries_per_conflict;
  double mean_rw_entries_per_conflict;
  vector<Conflict> additional_conflicts;
};

vector<Tx> generatePredefinedConflicts(PredefinedConflictsConfig const& config,
                                       int64_t& generated_insns, int seed) {
  mt19937 gen(seed);

  normal_distribution<> insn_distr(20'000'000, 5'000'000);
  // uniform_int_distribution<> insn_distr(500'000, 100'000'000);
  // normal_distribution<> insn_distr(50'000'000, 5'000'000);
  uniform_int_distribution<> fee_distr(100, 1'000'000);

  vector<Tx> res;
  int entry_id = 0;
  generated_insns = 0;
  int txId = 0;
  while (generated_insns < config.total_insns) {
    int insns =
        max(500'000, min(100'000'000, static_cast<int>(insn_distr(gen))));
    auto& tx = res.emplace_back(txId++, insns, fee_distr(gen));
    generated_insns += tx.insns;
  }

  int conflict_count = round(config.conflicts_per_tx * res.size());
  poisson_distribution<> ro_count_distr(config.mean_ro_entries_per_conflict);
  poisson_distribution<> rw_count_distr(config.mean_rw_entries_per_conflict);
  auto generateConflict = [&](int from, int ro_count, int rw_count) {
    ++entry_id;
    for (int j = from; j < from + ro_count; ++j) {
      res[j].read_only.push_back(entry_id);
    }
    for (int j = from + ro_count; j < from + ro_count + rw_count; ++j) {
      res[j].read_write.push_back(entry_id);
    }
    res[from + ro_count + rw_count - 1].fee += 1'000'000;
  };
  while (conflict_count > 0) {
    shuffle(res.begin(), res.end(), gen);
    int roCount = ro_count_distr(gen);
    int rwCount = max(1, rw_count_distr(gen));
    generateConflict(0, roCount, rwCount);
    conflict_count -= roCount + rwCount;
  }
  shuffle(res.begin(), res.end(), gen);
  int from_id = 0;
  for (const auto& conflict : config.additional_conflicts) {
    int ro_count = conflict.fraction_of_txs_ro * res.size();
    int rw_count =
        max(1, static_cast<int>(conflict.fraction_of_txs_rw * res.size()));
    generateConflict(from_id, ro_count, rw_count);
    from_id += ro_count + rw_count;
  }

  return res;
}

PredefinedConflictsConfig predefinedConflicts(
    double conflicts_per_tx, double mean_ro_entries_per_conflict,
    double mean_rw_entries_per_conflict,
    vector<Conflict> additional_conflicts) {
  PredefinedConflictsConfig cfg;
  cfg.total_insns = INSNS_PER_LEDGER * 2;
   //cfg.total_insns = INSNS_PER_LEDGER;
  cfg.conflicts_per_tx = conflicts_per_tx;
  cfg.mean_ro_entries_per_conflict = mean_ro_entries_per_conflict;
  cfg.mean_rw_entries_per_conflict = mean_rw_entries_per_conflict;
  cfg.additional_conflicts = additional_conflicts;
  return cfg;
}

void smokeTest(PartitionConfig cfg) {
  int64_t generated_insns = 0;
  // Oracles
   auto txs = generatePredefinedConflicts(
     predefinedConflicts(10, 50, 5, {Conflict(0.9, 0.0)}), generated_insns,
     123);
  // Arbitrage
   //auto txs = generatePredefinedConflicts(
   //   predefinedConflicts(10, 50, 5, {Conflict(0.0, 0.8)}), generated_insns,
   //   123);
  //auto txs = generatePredefinedConflicts(predefinedConflicts(10, 10, 5, {}),
  //                                       generated_insns, 123);
  cout << "Tx count: " << txs.size() << endl;
  int iter = 0;
  while (!txs.empty()) {
    auto start = high_resolution_clock::now();
    auto tx_set = partition(txs, cfg);
    auto stop = high_resolution_clock::now();
    tx_set.validate();
    auto duration = duration_cast<milliseconds>(stop - start);
    cout << "Partition time: " << duration.count() << " ms" << endl;
    int64_t insns_left = 0;
    for (const auto& tx : txs) {
      insns_left += tx.insns;
    }
    cout << "Init percent left: " << 100.0 * insns_left / generated_insns
         << endl;
    cout << "Utilization: "
         << 100.0 * (generated_insns - insns_left) / cfg.maxInsns() << endl;
    ++iter;
  }
  cout << "Total iter: " << iter << endl;
}

double benchmark(const PartitionConfig& partition_cfg,
                 const PredefinedConflictsConfig& gen_cfg) {
  const int ITERATIONS = 10;
  double utilization = 0;
  for (int iter = 0; iter < ITERATIONS; ++iter) {
    int64_t generated_insns = 0;
    auto txs = generatePredefinedConflicts(gen_cfg, generated_insns, iter);
    auto tx_set = partition(txs, partition_cfg);
    tx_set.validate();
    int64_t insns_left = 0;
    for (const auto& tx : txs) {
      insns_left += tx.insns;
    }
    utilization += static_cast<double>(generated_insns - insns_left) /
                   partition_cfg.maxInsns();
  }
  return utilization / ITERATIONS;
}

PartitionConfig partitionConfig(int stage_count) {
  PartitionConfig cfg;
  cfg.stage_count = stage_count;
  cfg.threads_per_stage = THREADS;
  cfg.insns_per_thread = INSNS_PER_THREAD / stage_count;
  return cfg;
}

vector<double> stageBenchmarks(const PredefinedConflictsConfig& gen_cfg) {
  vector<double> res;
  for (int stage_count = 1; stage_count <= 4; ++stage_count) {
    auto cfg = partitionConfig(stage_count);
    res.push_back(benchmark(cfg, gen_cfg));
  }
  return res;
}

void randomTrafficBenchmarks() {
  ofstream out("random_traffic.csv");
  out << "conflicts_per_tx,mean_ro_entries_per_conflict,mean_rw_entries_per_"
         "conflict,1_stage,2_stage,3_stage,4_stage"
      << endl;
  for (int conflicts_per_tx = 0; conflicts_per_tx <= 50;
       conflicts_per_tx += 5) {
    for (int mean_ro_entries = 10; mean_ro_entries <= 50;
         mean_ro_entries += 10) {
      for (int mean_rw_entries = 1; mean_rw_entries <= 5;
           mean_rw_entries += 2) {
        out << conflicts_per_tx << "," << mean_ro_entries << ","
            << mean_rw_entries << ",";
        auto stage_benchmarks = stageBenchmarks(predefinedConflicts(
            conflicts_per_tx, mean_ro_entries, mean_rw_entries, {}));
        for (double v : stage_benchmarks) {
          out << v << ",";
        }
        out << endl;
      }
    }
  }
}

void randomTrafficBenchmarksRw() {
  ofstream out("random_traffic_rw.csv");
  out << "conflicts_per_tx,mean_rw_entries_per_"
         "conflict,1_stage,2_stage,3_stage,4_stage"
      << endl;
  for (int conflicts_per_tx = 0; conflicts_per_tx <= 50;
       conflicts_per_tx += 5) {
    for (int mean_rw_entries = 5; mean_rw_entries <= 30; mean_rw_entries += 5) {
      out << conflicts_per_tx << "," << mean_rw_entries << ",";
      auto stage_benchmarks = stageBenchmarks(
          predefinedConflicts(conflicts_per_tx, 0, mean_rw_entries, {}));
      for (double v : stage_benchmarks) {
        out << v << ",";
      }
      out << endl;
    }
  }
}

void oracleBenchmarks() {
  ofstream out("oracle_update.csv");
  out << "num_oracles,read_tx_fraction,1_stage,2_stage,3_stage,4_stage" << endl;
  const int ReadTxFractionStep = 0.2;
  for (int num_oracles = 1; num_oracles <= 3; ++num_oracles) {
    for (int read_tx_fraction_mult = 1; read_tx_fraction_mult <= 3;
         ++read_tx_fraction_mult) {
      double read_tx_fraction = read_tx_fraction_mult * 0.3;
      out << num_oracles << "," << read_tx_fraction << ",";
      vector<Conflict> oracles;
      for (int i = 0; i < num_oracles; ++i) {
        oracles.emplace_back(read_tx_fraction / num_oracles, 0.0);
      }
      auto stage_benchmarks =
          stageBenchmarks(predefinedConflicts(10, 50, 5, oracles));
      for (double v : stage_benchmarks) {
        out << v << ",";
      }
      out << endl;
    }
  }
}

void arbitrageBenchmarks() {
  ofstream out("arbitrage.csv");
  out << "txs_fraction,conflict_clusters,1_stage,2_stage,3_stage,4_stage"
      << endl;
  const int ReadTxFractionStep = 0.2;
  for (int txs_fraction_mult = 1; txs_fraction_mult <= 9; ++txs_fraction_mult) {
    double txs_fraction = txs_fraction_mult * 0.1;
    for (int conflict_clusters = 1; conflict_clusters <= 3;
         conflict_clusters += 2) {
      out << txs_fraction << "," << conflict_clusters << ",";
      vector<Conflict> conflicts;
      for (int i = 0; i < conflict_clusters; ++i) {
        conflicts.emplace_back(0.0, txs_fraction / conflict_clusters);
      }
      auto stage_benchmarks =
          stageBenchmarks(predefinedConflicts(10, 50, 5, conflicts));
      for (double v : stage_benchmarks) {
        out << v << ",";
      }
      out << endl;
    }
  }
}

int main() {
  // auto cfg = partitionConfig(2);
  //  cout << benchmark(cfg, predefinedConflicts(cfg.maxInsns() * 2, 1, 10, 2,
  //                                             {Conflict(0.0, 0.8)}))
  //       << endl;
  //  cout << benchmark(cfg, predefinedConflicts(cfg.maxInsns() * 2, 10, 100,
  //  10,
  //                                             {}))
  //       << endl;
  //  cout << benchmark(cfg, predefinedConflicts(cfg.maxInsns() * 2, 10, 10, 1,
  //                                            {Conflict(0.9, 0.0)}))
  //      << endl;

  randomTrafficBenchmarks();
  //randomTrafficBenchmarksRw();
  //oracleBenchmarks();
  //arbitrageBenchmarks();

   //smokeTest(partitionConfig(4));
}

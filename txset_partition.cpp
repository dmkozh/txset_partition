#include <algorithm>
#include <chrono>
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
    auto packing = binPacking(new_clusters);
    if (packing.empty()) {
      return nullopt;
    }
    if (do_add) {
      clusters_ = new_clusters;
      packing_ = packing;
      total_insns_ += tx.insns;
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
   //sort(txs.begin(), txs.end(),
   //     [](const Tx& a, const Tx& b) { return a.insns > b.insns; });
  vector<Stage> stages(cfg.stage_count, cfg);

  for (const auto& tx : txs) {
    int min_conflicting_insns = numeric_limits<int>::max();
    Stage* best_stage = nullptr;
    for (auto& stage : stages) {
      auto maybe_conflicting_insns = stage.tryAdd(tx, false);
      if (maybe_conflicting_insns &&
          *maybe_conflicting_insns < min_conflicting_insns) {
        min_conflicting_insns = *maybe_conflicting_insns;
        best_stage = &stage;
      }
    }
    if (best_stage != nullptr) {
      best_stage->tryAdd(tx, true);
    }
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
                                       int64_t& generated_insns) {
  mt19937 gen(123);

  normal_distribution<> insn_distr(20'000'000, 5'000'000);

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

vector<Tx> predefinedConflicts(int64_t max_insns, double conflicts_per_tx,
                               double mean_ro_entries_per_conflict,
                               double mean_rw_entries_per_conflict,
                               vector<Conflict> additional_conflicts,
                               int64_t& generated_insns) {
  PredefinedConflictsConfig cfg;
  cfg.total_insns = max_insns;
  cfg.conflicts_per_tx = conflicts_per_tx;
  cfg.mean_ro_entries_per_conflict = mean_ro_entries_per_conflict;
  cfg.mean_rw_entries_per_conflict = mean_rw_entries_per_conflict;
  cfg.additional_conflicts = additional_conflicts;
  return generatePredefinedConflicts(cfg, generated_insns);
}

int main() {
  ParitionConfig cfg;
  cfg.stage_count = 4;
  cfg.threads_per_stage = 8;
  cfg.insns_per_thread = 125'000'000;

  int64_t generated_insns = 0;
  // Oracles
  // auto txs = predefinedConflicts(cfg.maxInsns() * 2, 0.5, 10, 2,
  //                               {Conflict(0.25, 0), Conflict(0.25, 0)},
  //                               generated_insns);
  // Arbitrage
  auto txs = predefinedConflicts(cfg.maxInsns() * 2, 0.5, 10, 2,
                                 {Conflict(0.0, 0.8)}, generated_insns);
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
    cout << "Utilization: "
         << 100.0 * (generated_insns - insns_left) / cfg.maxInsns() << endl;
    ++iter;
  }
  cout << "Total iter: " << iter << endl;
}

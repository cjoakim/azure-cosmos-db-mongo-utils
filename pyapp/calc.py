"""
Ad-hoc storage and RU calculation.
Usage:
    python calc.py
"""

if __name__ == "__main__":
    writes_per_day   = float(50_000_000)
    write_read_ratio = float(100)
    reads_per_day    = float(writes_per_day / write_read_ratio)
    seconds_per_day  = float(60 * 60 * 24)
    writes_per_sec   = writes_per_day / seconds_per_day
    reads_per_sec    = reads_per_day / seconds_per_day
    msg_size_kb      = float(7.5)
    write_ru_per_sec = float(writes_per_sec * msg_size_kb * 5)
    read_ru_per_sec  = float(reads_per_sec * msg_size_kb * 1)
    total_ru_sec     = float(write_ru_per_sec + read_ru_per_sec)
    spike_multiplier = 3.0
    spike_ru_sec     = total_ru_sec * spike_multiplier
    cosmos_ttl_days  = 1.0
    sl_ttl_days      = 14.0
    kb_per_day       = writes_per_day * msg_size_kb
    mb_per_day       = kb_per_day / 1024.0
    gb_per_day       = mb_per_day / 1024.0
    total_cosmos_gb_storage = gb_per_day * cosmos_ttl_days
    total_cosmos_doc_count  = writes_per_day * cosmos_ttl_days
    total_sl_gb_storage     = gb_per_day * sl_ttl_days
    total_sl_doc_count      = writes_per_day * sl_ttl_days
    sl_storage_cost_month   = total_sl_gb_storage * 0.03
    transactions_per_month  = float((writes_per_day + reads_per_day) * 30.0)
    sl_trans_cost_month     = float(transactions_per_month) / 10000.0 * 0.005
    # https://azure.microsoft.com/en-us/pricing/details/cosmos-db/autoscale-provisioned/

    physical_partition_size_gb = 50.0
    physical_partitions = total_cosmos_gb_storage / physical_partition_size_gb
    max_ru_per_physical_partition = 10_000
    total_ru_per_total_storage_size = physical_partitions * max_ru_per_physical_partition

    print("")
    print("Ingestion RU Calculation:")
    print("  regions:                         {}".format(1))
    print("  writes_per_day:                  {}".format(writes_per_day))
    print("  write_read_ratio:                {}".format(write_read_ratio))
    print("  reads_per_day:                   {}".format(reads_per_day))
    print("  writes_per_sec:                  {}".format(writes_per_sec))
    print("  reads_per_sec:                   {}".format(reads_per_sec))
    print("  msg_size_kb:                     {}".format(msg_size_kb))
    print("  write_ru_per_sec:                {}".format(write_ru_per_sec))
    print("  read_ru_per_sec:                 {}".format(read_ru_per_sec))
    print("  total_ru_sec:                    {}".format(total_ru_sec))
    print("  spike_multiplier:                {}".format(spike_multiplier))
    print("  spike_ru_sec:                    {}".format(spike_ru_sec))
    print("")
    print("Cosmos DB Storage RU Calculation:")
    print("  cosmos_ttl_days:                 {}".format(cosmos_ttl_days))
    print("  documents_per_day:               {}".format(writes_per_day))
    print("  total_cosmos_doc_count:          {}".format(total_cosmos_doc_count))
    print("  gb_per_day:                      {}".format(gb_per_day))
    print("  total_cosmos_gb_storage:         {}".format(total_cosmos_gb_storage))
    print("  physical_partition_size_gb:      {}".format(physical_partition_size_gb))
    print("  physical_partitions:             {}".format(physical_partitions))
    print("  max_ru_per_physical_partition:   {}".format(max_ru_per_physical_partition))
    print("  total_ru_per_total_storage_size: {}".format(total_ru_per_total_storage_size))
    print("")
    print("Synapse Link Calculation:")
    print("  sl_ttl_days:                     {}".format(sl_ttl_days))
    print("  documents_per_day:               {}".format(writes_per_day))
    print("  total_sl_doc_count:              {}".format(total_sl_doc_count))
    print("  gb_per_day:                      {}".format(gb_per_day))
    print("  total_sl_gb_storage:             {}".format(total_sl_gb_storage))
    print("  sl_storage_cost_month:           {}".format(sl_storage_cost_month))
    print("  transactions_per_month:          {}".format(transactions_per_month))
    print("  sl_trans_cost_month:             {}".format(sl_trans_cost_month))
    print("")
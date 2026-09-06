#pragma once

#include "../System.h"

#include <limits>


class ReplicationSystem : public System {
public:
    ReplicationSystem(TaskGraph&& task_graph, Platform const& platform)
        : task_graph(std::move(task_graph)),
        platform(platform)
    {}


    Time computation_time_ms(
        Task* task,
        Processor const* processor) const override
    {
        Time time =
            processor->processing_time_ms(
                task->get_input_size(),
                task->get_parallelizability()
            ) * task->get_complexity();

        if (processor->is_streaming_device()) {
            time /= task->get_streamability();
        }

        return time;
    }


    Time transaction_time_ms(
        DataSize const& transfer_size_MB,
        Device const* dev1,
        Device const* dev2) const override
    {
        DataRate transfer_rate_MBps =
            platform.transfer_rate_MBps(dev1, dev2);

        if (transfer_rate_MBps == 0) {
            return std::numeric_limits<Time>::infinity();
        }

        if (transfer_rate_MBps == std::numeric_limits<DataRate>::infinity()) {
            return 0;
        }

        return 1000 * static_cast<Time>(transfer_size_MB) / transfer_rate_MBps;
    }


    /*
     * Compatibility was already checked against the original System
     * while MOVE/REPLICATE candidates were generated.
     *
     * The materialized DAG may change which concrete replicas appear
     * as sources or sinks, so compatibility should not be re-evaluated
     * using the materialized graph structure.
     */
    bool is_compatible(Task*, Device const*) const override
    {
        return true;
    }


    TaskGraph const& get_task_graph() const override
    {
        return task_graph;
    }


    Platform const& get_platform() const override
    {
        return platform;
    }


private:
    TaskGraph task_graph;
    Platform const& platform;
};
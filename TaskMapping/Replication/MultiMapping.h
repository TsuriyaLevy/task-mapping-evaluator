#pragma once

#include "../System.h"

#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <iostream>


class MultiMapping {
public:
    struct DeviceTriplet {
        Processor const* processor;
        Memory const* memory_in;
        Memory const* memory_out;

        bool operator==(DeviceTriplet const& other) const {
            return processor == other.processor;
        }
    };


    struct DeviceTripletHash {
        std::size_t operator()(DeviceTriplet const& triplet) const {
            return std::hash<Processor const*>{}(triplet.processor);
        }
    };


    using ReplicaSet = std::unordered_set<DeviceTriplet, DeviceTripletHash>;


private:
    std::unordered_map<Task*, ReplicaSet> mapping;


public:
    void add_replica(
        Task* task,
        Processor const* processor,
        Memory const* mem_in,
        Memory const* mem_out)
    {
        mapping[task].insert({ processor, mem_in, mem_out });
    }


    void add_replica(Task* task, Processor const* processor)
    {
        add_replica(
            task,
            processor,
            processor->get_default_memory(),
            processor->get_default_memory()
        );
    }


    bool has_replica(Task* task, Processor const* processor) const
    {
        auto it = mapping.find(task);
        if (it == mapping.end()) return false;

        return it->second.contains({ processor, nullptr, nullptr });
    }


    ReplicaSet const& get_replicas(Task* task) const
    {
        return mapping.at(task);
    }


    ReplicaSet const* try_get_replicas(Task* task) const
    {
        auto it = mapping.find(task);
        if (it == mapping.end()) return nullptr;

        return &it->second;
    }


    DeviceTriplet const* get_replica(Task* task, Processor const* processor) const
    {
        auto it = mapping.find(task);
        if (it == mapping.end()) return nullptr;

        for (DeviceTriplet const& replica : it->second) {
            if (replica.processor == processor) return &replica;
        }

        return nullptr;
    }


    /*
     * MOVE removes all previous replicas and leaves only the
     * requested mapping.
     */
    void move_task(
        Task* task,
        Processor const* processor,
        Memory const* mem_in,
        Memory const* mem_out)
    {
        ReplicaSet& replicas = mapping[task];

        replicas.clear();
        replicas.insert({ processor, mem_in, mem_out });
    }


    void move_task(Task* task, Processor const* processor)
    {
        move_task(
            task,
            processor,
            processor->get_default_memory(),
            processor->get_default_memory()
        );
    }


    void replicate_subgraph(
        std::vector<Task*> const& subgraph,
        Processor const* processor,
        Memory const* mem_in,
        Memory const* mem_out)
    {
        for (Task* task : subgraph) {
            add_replica(task, processor, mem_in, mem_out);
        }
    }


    void replicate_subgraph(
        std::vector<Task*> const& subgraph,
        Processor const* processor)
    {
        replicate_subgraph(
            subgraph,
            processor,
            processor->get_default_memory(),
            processor->get_default_memory()
        );
    }


    void move_subgraph(
        std::vector<Task*> const& subgraph,
        Processor const* processor,
        Memory const* mem_in,
        Memory const* mem_out)
    {
        for (Task* task : subgraph) {
            move_task(task, processor, mem_in, mem_out);
        }
    }


    void move_subgraph(
        std::vector<Task*> const& subgraph,
        Processor const* processor)
    {
        move_subgraph(
            subgraph,
            processor,
            processor->get_default_memory(),
            processor->get_default_memory()
        );
    }


    bool validate(System const& sys) const
    {
        for (Task* task : sys.get_task_graph().get_tasks()) {
            auto it = mapping.find(task);

            if (it == mapping.end() || it->second.empty()) {
                std::cerr
                    << "MultiMapping validation failed: task "
                    << task->get_label()
                    << " has no replicas."
                    << std::endl;
                return false;
            }

            for (DeviceTriplet const& replica : it->second) {
                if (replica.processor == nullptr ||
                    replica.memory_in == nullptr ||
                    replica.memory_out == nullptr)
                {
                    std::cerr
                        << "MultiMapping validation failed: task "
                        << task->get_label()
                        << " has an invalid replica."
                        << std::endl;
                    return false;
                }

                if (!sys.is_compatible(task, replica.processor)) {
                    std::cerr
                        << "MultiMapping validation failed: task "
                        << task->get_label()
                        << " is incompatible with processor "
                        << replica.processor->get_label()
                        << "."
                        << std::endl;
                    return false;
                }
            }
        }

        return true;
    }
};
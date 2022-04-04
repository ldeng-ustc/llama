#ifndef LL_LOAD_BIN_H_
#define LL_LOAD_BIN_H_

#include <cstdint>
#include <sstream>
#include <string>
#include <vector>

#include "llama/ll_mem_array.h"
#include "llama/ll_writable_graph.h"

#include "llama/loaders/ll_load_async_writable.h"
#include "llama/loaders/ll_load_utils.h"


/**
 * Erdos-Renyi graph generator
 */
class ll_loader_bin : public ll_file_loader {

public:

	/**
	 * Create a new instance of ll_loader_bin
	 */
	ll_loader_bin() {}


	/**
	 * Destroy the instance
	 */
	virtual ~ll_loader_bin() {}


	/**
	 * Determine if this "file" can be opened by this loader
	 *
	 * @param file the file
	 * @return true if it can be opened
	 */
	virtual bool accepts(const char* file) {
        return strcmp(ll_file_extension(file), "bin") == 0;
	}


	/**
	 * Load directly into the read-only representation by creating a new
	 * level
	 *
	 * @param graph the graph
	 * @param file the file
	 * @param config the loader configuration
	 */
	virtual void load_direct(ll_mlcsr_ro_graph* graph, const char* file,
			const ll_loader_config* config) {

		bin_loader<uint64_t> loader(file);
		bool r = loader.load_direct(graph, config);
		if (!r) abort();
	}


	/**
	 * Load incrementally into the writable representation
	 *
	 * @param graph the graph
	 * @param file the file
	 * @param config the loader configuration
	 */
	virtual void load_incremental(ll_writable_graph* graph, const char* file,
			const ll_loader_config* config) {

		bin_loader<uint64_t> loader(file);
		bool r = loader.load_incremental(graph, config);
		if (!r) abort();
	}


	/**
	 * Create a data source object for the given file
	 *
	 * @param file the file
	 * @return the data source
	 */
	virtual ll_data_source* create_data_source(const char* file) {
		return new bin_loader<uint64_t>(file);
	}


private:

	/**
	 * The binary loader (no weights for now)
	 */
	template<typename NodeType>
	class bin_loader : public ll_edge_list_loader<NodeType, false>
	{	
		size_t _nodes;
		size_t _edges;

		FILE *_file;

	public:

		/**
		 * Create an instance of class bin_loader
		 *
		 * @param file_name the file name
		 */
		bin_loader(const char* file_name)
			: ll_edge_list_loader<uint64_t, false>(), _nodes(0), _edges(0), _loaded_edges(0) {
			_file = fopen(file_name, "rb");
		}


		/**
		 * Destroy the loader
		 */
		virtual ~bin_loader() {
		}


	protected:

		/**
		 * Read the next edge
		 *
		 * @param o_tail the output for tail
		 * @param o_head the output for head
		 * @param o_weight the output for weight (ignore if HasWeight is false)
		 * @return true if the edge was loaded, false if EOF or error
		 */
		virtual bool next_edge(NodeType* o_tail, NodeType* o_head,
				float* o_weight) override {
			NodeType nodes[2];
			fread(nodes, sizeof(NodeType), 2, _file);
			*o_tail = nodes[1];
			*o_head = nodes[0];
			_nodes = std::max(_nodes, nodes[0] + 1);
			_nodes = std::max(_nodes, nodes[1] + 1);
			_edges ++;
			return true;
		}


		/**
		 * Rewind the input file
		 */
		virtual void rewind() {
			fseek(_file, 0, SEEK_SET);
			_nodes = 0;
			_edges = 0;
		}


		/**
		 * Get graph stats if they are available
		 *
		 * @param o_nodes the output for the number of nodes (1 + max node ID)
		 * @param o_edges the output for the number of edges
		 * @return true if succeeded, or false if not or the info is not available
		 */
		bool stat(size_t* o_nodes, size_t* o_edges) {
			*o_nodes = _nodes;
			*o_edges = _edges;
			return true;
		}
	};
};

#endif
#ifndef LL_LOAD_BIN_H_
#define LL_LOAD_BIN_H_

#include <cstdint>
#include <sstream>
#include <string>
#include <vector>
#include <filesystem>
#include <utility>

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
        return std::filesystem::is_directory(file);;
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
		inline static const size_t BUFFER_SIZE = 1024 * 1024;
		using EdgeType = std::pair<NodeType, NodeType>;
		using DirIter = std::filesystem::directory_iterator;

		uint64_t _loaded_edges;

		EdgeType *_buffer;
		size_t _cur;
		size_t _count;

		const char* _dir;
		DirIter _iter;
		FILE *_file;

		bool open_file(const char* file) {
			_file = fopen(file, "rb");
			if(_file == NULL) {
				LL_E_PRINT("Open file '%s' failed.\n", file);
				abort();
			}
			return true;
		}

		bool next_file() {
			fclose(_file);
			if(++_iter == std::filesystem::end(_iter)) {
				return false;
			}
			return open_file(_iter->path().c_str());
		}

		bool read_buffer() {
			do{
				_count = fread(_buffer, sizeof(EdgeType), BUFFER_SIZE, _file);
				if(ferror(_file)) {
					LL_E_PRINT("Read file '%s' failed.\n", _iter->path().c_str());
					LL_I_PRINT("Loaded edges: %lu \n", _loaded_edges);
					abort();
				}
				if(feof(_file) && !next_file()) {
					return false;
				}
				_count = fread(_buffer, sizeof(EdgeType), BUFFER_SIZE, _file);
			} while (_count == 0);
			_cur = 0;
			return true;
		}

	public:

		/**
		 * Create an instance of class bin_loader
		 *
		 * @param file_name the file name
		 */
		bin_loader(const char* file_name)
			: ll_edge_list_loader<uint64_t, false>(), _dir(file_name), _file(NULL) {
			_buffer = new EdgeType[BUFFER_SIZE];
			rewind();
		}


		/**
		 * Destroy the loader
		 */
		virtual ~bin_loader() {
			delete _buffer;
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
			// read new data if all data in _buffer has been read. 
			if(_cur == _count && !read_buffer()) {
				return false;
			}
			*o_head = _buffer[_cur].first;
			*o_tail = _buffer[_cur].second;
			_cur++;
			_loaded_edges ++;
			return true;
		}

		/**
		 * Rewind the input file
		 */
		virtual void rewind() override {
			if(_file != NULL) {
				fclose(_file);
			}
			_iter = DirIter(_dir);
			if(_iter != std::filesystem::end(_iter)) {
				open_file(_iter->path().c_str());
			} else {
				LL_E_PRINT("No file in directory '%s'", _dir);
				abort();
			}
			_cur = _count = 0;
			_loaded_edges = 0;
		}


		/**
		 * Get graph stats if they are available
		 *
		 * @param o_nodes the output for the number of nodes (1 + max node ID)
		 * @param o_edges the output for the number of edges
		 * @return true if succeeded, or false if not or the info is not available
		 */
		virtual bool stat(size_t* o_nodes, size_t* o_edges) override {
			// *o_nodes = _nodes;
			// *o_edges = _edges;
			// return true;
			return false;
		}

	private:
		
	};
};

#endif
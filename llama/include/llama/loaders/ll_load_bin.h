#ifndef LL_LOAD_BIN_H_
#define LL_LOAD_BIN_H_

#include <cstdint>
#include <sstream>
#include <string>
#include <vector>
#include <filesystem>
#include <regex>
#include <utility>
#include <memory>

#include "llama/ll_mem_array.h"
#include "llama/ll_writable_graph.h"

#include "llama/loaders/ll_load_async_writable.h"
#include "llama/loaders/ll_load_utils.h"

const char* KRON_FILE_FORMAT_REGEX = R"(.*Kron(\d+)-(\d+)/?)";

/**
 * Erdos-Renyi graph generator
 */
class ll_loader_bin : public ll_file_loader {

	class kron_reader;

	std::unique_ptr<kron_reader> _reader;
	std::string _path;

	std::pair<uint64_t, uint64_t> init(const char* file, const ll_loader_config* config) {
		if(_reader == nullptr || _path != file) {
			_path = file;
			_reader = std::make_unique<kron_reader>(file);
		}

		uint64_t begin_edge = 0;
		uint64_t needed_edge = _reader->total_edges();

		if(config != nullptr && config->lc_partial_load_num_parts > 0) {
			auto nparts = config->lc_partial_load_num_parts;
			if(needed_edge % nparts != 0) {
				LL_E_PRINT("Can not split %lu edges into %lu parts\n", needed_edge, nparts);
				abort();
			}
			if (config->lc_partial_load_part <= 0
					|| (config->lc_partial_load_part > nparts)) {
				LL_E_PRINT("The partial load part ID is out of bounds\n");
				abort();
			}
			needed_edge /= nparts;
			begin_edge = (config->lc_partial_load_part - 1) * needed_edge;
		}
		return {needed_edge, begin_edge};
	}

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
        return std::filesystem::is_directory(file) && std::regex_match(file, std::regex(KRON_FILE_FORMAT_REGEX));
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
		auto [needed_edge, begin_edge] = init(file, config);
		printf("Load direct: neeeded: %lu, begin: %lu\n", needed_edge, begin_edge);
		bin_loader loader(_reader.get(), needed_edge, begin_edge);
		bool r = loader.load_direct(graph, config);
		if (!r) {
			LL_E_PRINT("Load direct failed!\n");
			abort();
		}
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
		auto [needed_edge, begin_edge] = init(file, config);
		bin_loader loader(_reader.get(), needed_edge, begin_edge);
		bool r = loader.load_incremental(graph, config);
		if (!r) {
			LL_E_PRINT("Load direct failed!\n");
			abort();
		}
	}


	/**
	 * Create a data source object for the given file
	 *
	 * @param file the file
	 * @return the data source
	 */
	virtual ll_data_source* create_data_source(const char* file) {
		auto [needed_edge, begin_edge] = init(file, nullptr);
		return new bin_loader(_reader.get(), needed_edge, begin_edge);
	}


private:
	using NodeType = uint64_t;
	using EdgeType = std::pair<NodeType, NodeType>;
	
	class kron_reader
	{
		inline static const size_t BUFFER_SIZE = 1024 * 1024;

		std::filesystem::path _path;
		EdgeType *_buffer;
		size_t _cur;			// cursor of buffer
		uint64_t _begin;			// order of first 
		uint64_t _len;			// edges count in buffer
		FILE* _file;
		size_t _cur_file_id;		
		
		uint64_t _total_nodes;
		uint64_t _total_edges;

		uint64_t _edges_per_file;

		std::string filename(size_t file_id) {
			const size_t MAX_FILENAME_LEN = 128;
			char name_buffer[MAX_FILENAME_LEN];
			sprintf(name_buffer, "block-%02lu.bin", file_id);
			return std::string(name_buffer);
		}

		std::filesystem::path filepath(size_t file_id) {
			return _path / filename(file_id);
		}

	public:
		kron_reader(std::string_view path): _path(path), _cur(0), _begin(0), _len(0), _file(nullptr), _cur_file_id(0) {
			_buffer = new EdgeType[BUFFER_SIZE];

			const std::string filename(path);
			std::smatch res;
			std::regex_search(filename, res, std::regex(KRON_FILE_FORMAT_REGEX));
			uint64_t n = std::stoul(res[1]);
			uint64_t m = std::stoul(res[2]);

			_total_nodes = 1ull << n;
			_total_edges = _total_nodes * m;

			_edges_per_file = std::filesystem::file_size(filepath(0)) / sizeof(EdgeType);

			//printf("total node: %lu,  total edges: %lu, _edges_per_file: %lu\n", _total_nodes, _total_edges, _edges_per_file);
		}

		~kron_reader() {
			delete [] _buffer;
		}

		// seek to the nth edge in dataset
		bool seek(size_t n) {
			// printf("Seek to the %luth edges\n", n);
			if(n >= _total_edges) {
				return false;
			}
			if (n >= _begin && n < _begin + _len) {
				_cur = n - _begin;
				return true;
			}
			size_t file_id = n / _edges_per_file;
			size_t file_off = n % _edges_per_file;

			if(_file == nullptr || file_id != _cur_file_id) {
				if(_file != nullptr) {
					fclose(_file);
				}
				_file = fopen(filepath(file_id).c_str(), "rb");
				if(_file == nullptr) {
					LL_E_PRINT("Open file '%s' failed.\n", filepath(file_id).c_str());
					abort();
				}
				_cur_file_id = file_id;
				// printf("Open file '%s'\n", filepath(file_id).c_str());
			}

			fseek(_file, file_off * sizeof(EdgeType), SEEK_SET);
			_len = fread(_buffer, sizeof(EdgeType), BUFFER_SIZE, _file);
			_begin = n;
			_cur = 0;
			if(ferror(_file) != 0) {
				LL_E_PRINT("Seek error: %s\n", std::strerror(ferror(_file)));
				abort();
			}
			return true;
		}

		bool next(NodeType* tail, NodeType* head) {
			if(_cur == _len) [[unlikely]] {
				bool ret = seek(_begin + _len);
				if(!ret) {
					return false;
				}
			}
			*tail = _buffer[_cur].second;
			*head = _buffer[_cur].first;
			_cur++;
			return true;
		}

		uint64_t total_edges() const {
			return _total_edges;
		}

		uint64_t total_nodes() const {
			return _total_nodes;
		}
		
	};

	/**
	 * The binary loader (no weights for now)
	 */
	class bin_loader : public ll_edge_list_loader<NodeType, false>
	{
		inline static const size_t BUFFER_SIZE = 1024 * 1024;

		uint64_t _begin_edge;
		uint64_t _needed_edges;
		uint64_t _loaded_edges;
		kron_reader *_reader;

	public:

		/**
		 * Create an instance of class bin_loader
		 *
		 * @param file_name the file name
		 */
		bin_loader(kron_reader* reader, uint64_t needed, uint64_t begin)
			: ll_edge_list_loader<uint64_t, false>(), _begin_edge(begin), _needed_edges(needed), _loaded_edges(0), _reader(reader) {
			rewind();
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
			if(_loaded_edges == _needed_edges) {
				return false;
			}
			bool ok = _reader->next(o_tail, o_head);
			_loaded_edges += ok;
			// printf("loaded %5ld edges: %15lu %15lu  ok: %d\n", _loaded_edges, *o_head, *o_tail, ok);
			return ok;
		}

		/**
		 * Rewind the input file
		 */
		virtual void rewind() override {
			_reader->seek(_begin_edge);
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
			*o_nodes = _reader->total_nodes();
			*o_edges = _needed_edges;
			return true;
		}

	private:
		
	};
};

#endif
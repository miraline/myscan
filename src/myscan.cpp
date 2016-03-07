#include "server_http.hpp"
#include "client_http.hpp"

#include <fstream>
#include "storage.hpp"
#include "utils.hpp"

using namespace std;
using namespace utils;
using namespace boost::property_tree;

typedef SimpleWeb::Server<SimpleWeb::HTTP> HttpServer;
typedef SimpleWeb::Client<SimpleWeb::HTTP> HttpClient;

int main() {
	//HTTP-server at port 8080 using 4 threads
	try {
		MyScan::Storage storage;

		string address = storage.getCfgStr("address", "");
		int port = storage.getCfgInt("port", 8080);
		int threads = storage.getCfgInt("threads", 4);
		int timeoutConnect = storage.getCfgInt("timeout_connect", 5);
		int timeoutData = storage.getCfgInt("timeout_connect", 30);
		log("Listening at "+address+":"+to_string(port)+" with "+to_string(threads)+" threads");
		HttpServer server(address, port, threads, timeoutConnect, timeoutData);
		
		//Add resources using path-regex and method-string, and an anonymous function
		//POST-example for the path /string, responds the posted string
		server.resource["^/scan$"]["POST"]=[&](HttpServer::Response& response, shared_ptr<HttpServer::Request> request) {
			utils::Timer timer;
			string input = request->content.string(), output;
			int code = 200;
			try {
				boost::posix_time::ptime mst1 = boost::posix_time::microsec_clock::local_time();
				ptree pt;
				istringstream ss(input);
				read_json(ss, pt);

				MyScan::Request req(pt, storage);
				MyScan::Response resp;
				storage.query(req, resp);
				output = resp.toJSON(req);

				boost::posix_time::ptime mst2 = boost::posix_time::microsec_clock::local_time();
				boost::posix_time::time_duration msdiff = mst2 - mst1;
				resp.milliSec = msdiff.total_milliseconds();

				log( request->remote_endpoint_address + ":" + to_string(request->remote_endpoint_port) + " input:'" + input + "' ids:" + to_string( resp.ids.size() )+ " rows:" +  to_string(resp.rowsMatch) + " total:" + to_string(resp.rowsMatch) + " slot:" + to_string(resp.activeSlotIndex) + " api_key:" + req.APIKey + " time:" + to_string(resp.milliSec) + "ms");
			} catch (runtime_error &e) {
				log( request->remote_endpoint_address + ":" + to_string(request->remote_endpoint_port) + " input:'" + input + "' RUNTIME_ERROR:'"+e.what()+"'");
				err(string("Query Runtime Error: ") + e.what());
				output = "{\"status\": \"error\", \"error\":\""+string(e.what())+"\"}\n";
				code = 500;
			} catch (exception &e) {
				log( request->remote_endpoint_address + ":" + to_string(request->remote_endpoint_port) + " input:'" + input + "' EXCEPTION:'"+e.what()+"'");
				err(string("Query Exception: ") + e.what());
				output = "{\"status\": \"error\", \"error\":\""+string(e.what())+"\"}\n";
				code = 500;
			} catch (...) {
				log( request->remote_endpoint_address + ":" + to_string(request->remote_endpoint_port) + " input:'" + input + "' UNKNOWN EXCEPTION");
				err("Query Unknown Exception");
				output = "{\"status\": \"error\", \"error\":\"UNKNOWN ERROR\"}\n";
				code = 500;
			}

			response << "HTTP/1.1 "+to_string(code)+" OK\r\nContent-Type: application/json\r\nContent-Length: " << output.length() << "\r\n\r\n" << output;
			//log(string("Total time to process request: ") + to_string( timer.elapsed() ) + "ms");
		};
		
		//POST-example for the path /json, responds firstName+" "+lastName from the posted json
		//Responds with an appropriate error message if the posted json is not valid, or if firstName or lastName is missing
		//Example posted json:
		//{
		//  "firstName": "John",
		//  "lastName": "Smith",
		//  "age": 25
		//}
		server.resource["^/echo$"]["POST"]=[](HttpServer::Response& response, shared_ptr<HttpServer::Request> request) {
			try {
				ptree pt;
				read_json(request->content, pt);

				string output=pt.get<string>("query");

				response << "HTTP/1.1 200 OK\r\nContent-Length: " << output.length() << "\r\n\r\n" << output;
			}
			catch(exception& e) {
				response << "HTTP/1.1 400 Bad Request\r\nContent-Length: " << strlen(e.what()) << "\r\n\r\n" << e.what();
			}
		};
		
		//GET-example for the path /info
		//Responds with request-information
		server.resource["^/info$"]["GET"]=[](HttpServer::Response& response, shared_ptr<HttpServer::Request> request) {
			stringstream content_stream;
			content_stream << "<h1>Request from " << request->remote_endpoint_address << " (" << request->remote_endpoint_port << ")</h1>";
			content_stream << request->method << " " << request->path << " HTTP/" << request->http_version << "<br>";
			for(auto& header: request->header) {
				content_stream << header.first << ": " << header.second << "<br>";
			}
			
			//find length of content_stream (length received using content_stream.tellp())
			content_stream.seekp(0, ios::end);
			
			response <<  "HTTP/1.1 200 OK\r\nContent-Length: " << content_stream.tellp() << "\r\n\r\n" << content_stream.rdbuf();
		};
		
		//Default GET-example. If no other matches, this anonymous function will be called. 
		//Will respond with content in the web/-directory, and its subdirectories.
		//Default file: index.html
		//Can for instance be used to retrieve an HTML 5 client that uses REST-resources on this server
		server.default_resource["GET"]=[](HttpServer::Response& response, shared_ptr<HttpServer::Request> request) {
			boost::filesystem::path web_root_path("web");
			if(!boost::filesystem::exists(web_root_path))
				cerr << "Could not find web root." << endl;
			else {
				auto path=web_root_path;
				path+=request->path;
				if(boost::filesystem::exists(path)) {
					if(boost::filesystem::canonical(web_root_path)<=boost::filesystem::canonical(path)) {
						if(boost::filesystem::is_directory(path))
							path+="/index.html";
						if(boost::filesystem::exists(path) && boost::filesystem::is_regular_file(path)) {
							ifstream ifs;
							ifs.open(path.string(), ifstream::in | ios::binary);
							
							if(ifs) {
								ifs.seekg(0, ios::end);
								size_t length=ifs.tellg();
								
								ifs.seekg(0, ios::beg);
								
								response << "HTTP/1.1 200 OK\r\nContent-Length: " << length << "\r\n\r\n";
								
								//read and send 128 KB at a time
								size_t buffer_size=131072;
								vector<char> buffer;
								buffer.reserve(buffer_size);
								size_t read_length;
								try {
									while((read_length=ifs.read(&buffer[0], buffer_size).gcount())>0) {
										response.write(&buffer[0], read_length);
										response.flush();
									}
								}
								catch(const exception &e) {
									cerr << "Connection interrupted, closing file" << endl;
								}

								ifs.close();
								return;
							}
						}
					}
				}
			}
			string content="Could not open path "+request->path;
			response << "HTTP/1.1 400 Bad Request\r\nContent-Length: " << content.length() << "\r\n\r\n" << content;
		};
		
		thread server_thread([&server](){
			server.start();
		});

		signal(SIGHUP, MyScan::Storage::sigHupHandler);
		//signal(SIGINT, Storage::sigIntHandler);
		log("Application started");

		//Wait for server to start so that the client can connect
		this_thread::sleep_for(chrono::seconds(1));
		
		//Client examples
		HttpClient client("localhost:8080");

		string json_string="{\"query\": \"ALIVE\"}";
		auto r3=client.request("POST", "/echo", json_string);
		cout << r3->content.rdbuf() << endl;
			
		server_thread.join();
		log("Waiting for update worker to stop");
		storage.exitRequested = true;
		//update_thread.join();

	} catch(const ParseException &e) {
		cerr << "Could not start. Config parse error at " << e.getFile() << ":" << e.getLine() << " - " << e.getError() << std::endl;
	} catch(const exception &e) {
		cerr << "Could not start: exception: " << e.what() << endl;
	} catch(...) {
		cerr << "Could not start: UNKNOWN EXCEPTION" << endl;
	}
	
	return 0;
}

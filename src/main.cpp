#include <pthread.h>
#include <signal.h>
#include <unistd.h>

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <string>

#include "chronofeed/config.hpp"
#include "chronofeed/log.hpp"
#include "chronofeed/pipeline.hpp"

namespace {

void usage(const char* argv0) {
  std::fprintf(stderr,
               "usage: %s [-c config_file] [-d duration_s]\n"
               "  -c  path to key=value config (default: config/chronofeed.conf if present)\n"
               "  -d  run for N seconds then exit (default: until SIGINT/SIGTERM)\n"
               "Environment overrides: CHRONOFEED_MQTT_HOST, CHRONOFEED_MQTT_PORT,\n"
               "  CHRONOFEED_MQTT_USERNAME, CHRONOFEED_MQTT_PASSWORD, CHRONOFEED_NTP_SERVER,\n"
               "  CHRONOFEED_DEADLINE_MS\n",
               argv0);
}

bool file_exists(const std::string& p) {
  if (std::FILE* f = std::fopen(p.c_str(), "r")) {
    std::fclose(f);
    return true;
  }
  return false;
}

}  // namespace

int main(int argc, char** argv) {
  std::string config_path;
  int duration_s = 0;

  for (int i = 1; i < argc; ++i) {
    if (std::strcmp(argv[i], "-c") == 0 && i + 1 < argc) {
      config_path = argv[++i];
    } else if (std::strcmp(argv[i], "-d") == 0 && i + 1 < argc) {
      duration_s = std::atoi(argv[++i]);
    } else {
      usage(argv[0]);
      return std::strcmp(argv[i], "-h") == 0 ? 0 : 2;
    }
  }
  if (config_path.empty() && file_exists("config/chronofeed.conf")) config_path = "config/chronofeed.conf";

  // Block termination signals in every thread; the main thread collects them with
  // sigwait so no async signal handler ever interrupts an RT thread.
  sigset_t sigs;
  sigemptyset(&sigs);
  sigaddset(&sigs, SIGINT);
  sigaddset(&sigs, SIGTERM);
  if (duration_s > 0) sigaddset(&sigs, SIGALRM);
  pthread_sigmask(SIG_BLOCK, &sigs, nullptr);
  signal(SIGPIPE, SIG_IGN);

  try {
    chronofeed::Config cfg = chronofeed::load_config(config_path);
    chronofeed::log::set_level(chronofeed::log::parse_level(cfg.log_level.c_str()));
    CF_LOG_INFO("chronofeed: config=%s broker=%s:%u topic=%s",
                config_path.empty() ? "(defaults)" : config_path.c_str(), cfg.mqtt.host.c_str(),
                cfg.mqtt.port, cfg.mqtt.topic.c_str());

    chronofeed::Pipeline pipeline(std::move(cfg));
    pipeline.start();

    if (duration_s > 0) alarm(static_cast<unsigned>(duration_s));
    int sig = 0;
    sigwait(&sigs, &sig);
    CF_LOG_INFO("chronofeed: received %s, shutting down", strsignal(sig));

    pipeline.stop();
    const auto r = pipeline.final_report();
    return r.missed == 0 ? 0 : 3;  // non-zero exit when the guarantee was violated
  } catch (const std::exception& e) {
    CF_LOG_ERROR("chronofeed: fatal: %s", e.what());
    return 1;
  }
}

#include "mandel/core.hpp"

#include <fstream>
#include <iostream>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include <nlohmann/json.hpp>
#include <toml++/toml.h>

using std::string;
using std::string_view;

namespace {

struct ArgSpec {
  string out_path = "mandelbrot.csv";
  mandel::Params p;
  bool show_help = false;
  std::optional<string> config_path{};
};

bool starts_with(string_view s, string_view prefix) {
  return s.size() >= prefix.size() && s.substr(0, prefix.size()) == prefix;
}

std::optional<string_view> value_for(string_view arg, string_view name) {
  // Accept: --name=value  or  --name value  (handled by caller)
  if (starts_with(arg, name) && arg.size() > name.size() &&
      arg[name.size()] == '=')
    return arg.substr(name.size() + 1);
  return std::nullopt;
}

void print_help(const char *argv0) {
  std::cout << "mandel_cli - minimal Mandelbrot CSV generator\n\n"
               "Usage:\n"
               "  "
            << argv0
            << " [--config file.{json,toml}]\n"
               "                 [--width N] [--height N]\n"
               "                 [--center-x X] [--center-y Y]\n"
               "                 [--scale S] [--max-iters N]\n"
               "                 [--out PATH]\n\n"
               "Notes:\n"
               "  Config values provide defaults; CLI flags override them.\n\n"
               "Defaults:\n"
               "  --width 300  --height 200  --center-x -0.75  --center-y 0.0\n"
               "  --scale 0.003  --max-iters 200  --out mandelbrot.csv\n";
}

// ------------------------- JSON config helpers -------------------------------

template <class T>
void maybe_set2(const nlohmann::json &j, const char *k1, const char *k2,
                T &dst) {
  if (j.contains(k1))
    dst = j.at(k1).get<T>();
  else if (j.contains(k2))
    dst = j.at(k2).get<T>();
}

void apply_json_config(const nlohmann::json &j, ArgSpec &a) {
  if (!j.is_object())
    throw std::runtime_error("Config root must be a JSON object");
  // Support both underscore and dash keys
  maybe_set2(j, "width", "width", a.p.width);
  maybe_set2(j, "height", "height", a.p.height);
  maybe_set2(j, "center_x", "center-x", a.p.center_x);
  maybe_set2(j, "center_y", "center-y", a.p.center_y);
  maybe_set2(j, "scale", "scale", a.p.scale);
  maybe_set2(j, "max_iters", "max-iters", a.p.max_iters);
  if (j.contains("out"))
    a.out_path = j.at("out").get<string>();
}

nlohmann::json load_json_file(const string &path) {
  std::ifstream f(path);
  if (!f)
    throw std::runtime_error("Failed to open config: " + path);
  nlohmann::json j;
  f >> j;
  return j;
}

// ------------------------- TOML config helpers
// --------------------------------

template <class T>
void toml_maybe_set2(const toml::table &t, const char *k1, const char *k2,
                     T &dst) {
  if (auto v = t[k1].template value<T>())
    dst = *v;
  else if (auto v2 = t[k2].template value<T>())
    dst = *v2;
}

void apply_toml_config(const toml::table &t, ArgSpec &a) {
  // Support both underscore and dash keys; in TOML, dashed keys must be quoted
  // in the file.
  toml_maybe_set2(t, "width", "width", a.p.width);
  toml_maybe_set2(t, "height", "height", a.p.height);
  toml_maybe_set2(t, "center_x", "center-x", a.p.center_x);
  toml_maybe_set2(t, "center_y", "center-y", a.p.center_y);
  toml_maybe_set2(t, "scale", "scale", a.p.scale);
  toml_maybe_set2(t, "max_iters", "max-iters", a.p.max_iters);
  if (auto v = t["out"].value<string>())
    a.out_path = *v;
}

toml::table load_toml_file(const string &path) {
  try {
    return toml::parse_file(path);
  } catch (const toml::parse_error &e) {
    throw std::runtime_error(std::string("TOML parse error: "));
  }
}

// --------------------------- CLI parsing -------------------------------------

int parse_int(string_view sv, const char *name) {
  try {
    return std::stoi(string(sv));
  } catch (...) {
    throw std::runtime_error(string("Invalid integer for ") + name + ": " +
                             string(sv));
  }
}

double parse_double(string_view sv, const char *name) {
  try {
    return std::stod(string(sv));
  } catch (...) {
    throw std::runtime_error(string("Invalid floating value for ") + name +
                             ": " + string(sv));
  }
}

std::string to_lower(std::string s) {
  for (char &c : s)
    c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
  return s;
}

ArgSpec parse_args(int argc, char **argv) {
  ArgSpec a;

  // Pass 1: find --config and apply it as defaults (before other flags).
  for (int i = 1; i < argc; ++i) {
    string_view cur(argv[i]);
    auto take_next = [&](const char *name) -> string {
      if (i + 1 >= argc)
        throw std::runtime_error(string("Missing value for ") + name);
      return string(argv[++i]);
    };
    if (cur == "--config") {
      a.config_path = take_next("--config");
    } else if (auto v = value_for(cur, "--config")) {
      a.config_path = string(*v);
    }
  }
  if (a.config_path) {
    auto ext =
        to_lower(a.config_path->substr(a.config_path->find_last_of('.') + 1));
    if (ext == "json") {
      apply_json_config(load_json_file(*a.config_path), a);
    } else if (ext == "toml") {
      apply_toml_config(load_toml_file(*a.config_path), a);
    } else {
      throw std::runtime_error("Unsupported config extension for --config: " +
                               *a.config_path + " (expected .json or .toml)");
    }
  }

  // Pass 2: parse/override with regular flags.
  for (int i = 1; i < argc; ++i) {
    string_view cur(argv[i]);

    if (cur == "--help" || cur == "-h") {
      a.show_help = true;
      continue;
    }
    if (cur == "--config" || starts_with(cur, "--config=")) {
      // already handled in pass 1
      if (cur == "--config")
        ++i; // skip the value
      continue;
    }

    auto need_next = [&](const char *name) -> string_view {
      if (i + 1 >= argc)
        throw std::runtime_error(string("Missing value for ") + name);
      return string_view(argv[++i]);
    };
    auto parse_opt = [&](string_view name, auto setter) {
      if (auto v = value_for(cur, name)) {
        setter(*v);
        return true;
      }
      if (cur == name) {
        setter(need_next(string(name).c_str()));
        return true;
      }
      return false;
    };

    if (parse_opt("--width",
                  [&](string_view v) { a.p.width = parse_int(v, "width"); }))
      continue;
    if (parse_opt("--height",
                  [&](string_view v) { a.p.height = parse_int(v, "height"); }))
      continue;
    if (parse_opt("--center-x", [&](string_view v) {
          a.p.center_x = parse_double(v, "center-x");
        }))
      continue;
    if (parse_opt("--center-y", [&](string_view v) {
          a.p.center_y = parse_double(v, "center-y");
        }))
      continue;
    if (parse_opt("--scale",
                  [&](string_view v) { a.p.scale = parse_double(v, "scale"); }))
      continue;
    if (parse_opt("--max-iters", [&](string_view v) {
          a.p.max_iters = parse_int(v, "max-iters");
        }))
      continue;
    if (parse_opt("--out", [&](string_view v) { a.out_path = string(v); }))
      continue;

    throw std::runtime_error("Unknown argument: " + string(cur));
  }

  if (a.p.width <= 0 || a.p.height <= 0)
    throw std::runtime_error("width/height must be positive.");
  if (a.p.max_iters <= 0)
    throw std::runtime_error("max-iters must be positive.");
  if (a.p.scale <= 0.0)
    throw std::runtime_error("scale must be positive.");
  return a;
}

} // namespace

int main(int argc, char **argv) {
  try {
    auto args = parse_args(argc, argv);
    if (args.show_help) {
      print_help(argv[0]);
      return 0;
    }

    std::vector<mandel::PixelResult> data;
    mandel::compute_grid(args.p, data);
    mandel::write_csv(args.out_path, data);
    return 0;
  } catch (const std::exception &e) {
    std::cerr << "Error: " << e.what() << "\nUse --help for usage.\n";
    return 1;
  }
}

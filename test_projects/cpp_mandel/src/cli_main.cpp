#include <mandel/core.hpp>

#include <iostream>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

using std::string;
using std::string_view;

namespace {

struct ArgSpec {
  string out_path = "mandelbrot.csv";
  mandel::Params p;
  bool show_help = false;
};

bool starts_with(string_view s, string_view prefix) {
  return s.size() >= prefix.size() && s.substr(0, prefix.size()) == prefix;
}

std::optional<string_view> value_for(string_view arg, string_view name) {
  // Accept: --name=value  or  --name value  (handled by caller)
  if (starts_with(arg, name) && arg.size() > name.size() &&
      arg[name.size()] == '=') {
    return arg.substr(name.size() + 1);
  }
  return std::nullopt;
}

void print_help(const char *argv0) {
  std::cout << "cppsim_cli - minimal Mandelbrot CSV generator\n\n"
               "Usage:\n"
               "  "
            << argv0
            << " [--width N] [--height N]\n"
               "                 [--center-x X] [--center-y Y]\n"
               "                 [--scale S] [--max-iters N]\n"
               "                 [--out PATH]\n\n"
               "Defaults:\n"
               "  --width 300  --height 200  --center-x -0.75  --center-y 0.0\n"
               "  --scale 0.003  --max-iters 200  --out mandelbrot.csv\n";
}

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

ArgSpec parse_args(int argc, char **argv) {
  ArgSpec a;
  for (int i = 1; i < argc; ++i) {
    string_view cur(argv[i]);

    if (cur == "--help" || cur == "-h") {
      a.show_help = true;
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
  if (a.p.width <= 0 || a.p.height <= 0) {
    throw std::runtime_error("width/height must be positive.");
  }
  if (a.p.max_iters <= 0) {
    throw std::runtime_error("max-iters must be positive.");
  }
  if (a.p.scale <= 0.0) {
    throw std::runtime_error("scale must be positive.");
  }
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

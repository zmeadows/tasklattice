#pragma once
#include <string>
#include <utility>
#include <vector>

namespace mandel {

struct Params {
  int width = 200;
  int height = 100;
  double center_x = -0.75;
  double center_y = 0.0;
  double scale = 0.003; // pixel-to-plane scale (smaller = more zoom)
  int max_iters = 200;
};

struct PixelResult {
  int px;
  int py;
  // Final z after the last iteration (escape or reaching max-iters).
  double x; // real(z_final)
  double y; // imag(z_final)
};

// Map pixel (px,py) to complex plane constant c = (cx, cy) using Params.
inline std::pair<double, double> map_pixel_to_plane(const Params &p, int px,
                                                    int py) {
  const double cx =
      p.center_x +
      (static_cast<double>(px) - static_cast<double>(p.width) / 2.0) * p.scale;
  const double cy =
      p.center_y +
      (static_cast<double>(py) - static_cast<double>(p.height) / 2.0) * p.scale;
  return {cx, cy};
}

// Return final z after iterating z_{n+1} = z_n^2 + c starting from z0 = 0
// for up to max_iters or until |z| > 2.
std::pair<double, double> mandelbrot_last_state(double cx, double cy,
                                                int max_iters);

// Compute full grid results into out (size: width*height). Deterministic,
// single-threaded. Each PixelResult stores the final z = (x,y) reached at
// termination.
void compute_grid(const Params &p, std::vector<PixelResult> &out);

// Write results to CSV path with header: px,py,x,y
// Throws on file I/O errors.
void write_csv(const std::string &path, const std::vector<PixelResult> &data);

} // namespace mandel

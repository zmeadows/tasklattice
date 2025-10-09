#include "mandel/core.hpp"
#include <fstream>
#include <stdexcept>

namespace mandel {

std::pair<double, double> mandelbrot_last_state(double cx, double cy,
                                                int max_iters) {
  double zr = 0.0, zi = 0.0;
  int it = 0;
  while (it < max_iters && (zr * zr + zi * zi) <= 4.0) {
    const double zr2 = zr * zr - zi * zi + cx;
    const double zi2 = 2.0 * zr * zi + cy;
    zr = zr2;
    zi = zi2;
    ++it;
  }
  return {zr, zi};
}

void compute_grid(const Params &p, std::vector<PixelResult> &out) {
  out.clear();
  out.reserve(static_cast<std::size_t>(p.width) *
              static_cast<std::size_t>(p.height));
  for (int py = 0; py < p.height; ++py) {
    for (int px = 0; px < p.width; ++px) {
      auto [cx, cy] = map_pixel_to_plane(p, px, py);
      auto [zr, zi] = mandelbrot_last_state(cx, cy, p.max_iters);
      out.push_back(PixelResult{px, py, zr, zi});
    }
  }
}

void write_csv(const std::string &path, const std::vector<PixelResult> &data) {
  std::ofstream ofs(path, std::ios::out | std::ios::trunc);
  if (!ofs) {
    throw std::runtime_error("Failed to open CSV for writing: " + path);
  }
  ofs << "px,py,x,y\n";
  for (const auto &r : data) {
    ofs << r.px << ',' << r.py << ',' << r.x << ',' << r.y << '\n';
  }
  if (!ofs) {
    throw std::runtime_error("I/O error while writing CSV: " + path);
  }
}

} // namespace mandel

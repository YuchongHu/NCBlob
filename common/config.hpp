#pragma once

namespace config {

#ifdef CFG_ENABLE_CONNECTION_POOL_
inline constexpr bool ENABLE_CONNECTION_POOL{true};
#else
inline constexpr bool ENABLE_CONNECTION_POOL{false};
#endif

#ifdef CFG_ENABLE_DEBUG_INFO
inline constexpr bool ENABLE_DEBUG_INFO{true};
#else
inline constexpr bool ENABLE_DEBUG_INFO{false};
#endif

#ifdef M_CFG_ENABLE_TRAFFIC_CONTROL
inline constexpr bool ENABLE_TRAFFIC_CONTROL{false};
#else
#endif

} // namespace config
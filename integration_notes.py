# Интеграция robot_screener.py в app.py
# =========================================
# Добавь в начало app.py:

from robot_screener import render_robot_screener_tab

# ─────────────────────────────────────────────
# В блоке где создаются вкладки, добавь новую:
# ─────────────────────────────────────────────

# Пример (адаптируй под свой код):
# tab1, tab2, tab3, tab_screener = st.tabs(["Скан", "Монитор", "Алго", "🎯 Ёрш-скринер"])

# В нужном месте основного цикла сохраняй algo_signals в session_state:
# if "algo_signals" not in st.session_state:
#     st.session_state.algo_signals = {}
# st.session_state.algo_signals[symbol] = algo_signal  # после каждого analyze_algo()

# Рендер вкладки:
# with tab_screener:
#     render_robot_screener_tab(
#         tracker=st.session_state.tracker,          # DensityTracker
#         algo_signals=st.session_state.get("algo_signals", {})  # dict[symbol, AlgoSignal]
#     )

# ─────────────────────────────────────────────
# МИНИМАЛЬНЫЙ ВАРИАНТ (без algo_signals):
# ─────────────────────────────────────────────
# render_robot_screener_tab(st.session_state.tracker)
# Работает только на данных стакана — без таймингов и честности.
# Для полного анализа нужны algo_signals из ws_monitor или REST.

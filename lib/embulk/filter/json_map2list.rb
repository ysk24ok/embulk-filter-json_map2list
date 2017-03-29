Embulk::JavaPlugin.register_filter(
  "json_map2list", "org.embulk.filter.json_map2list.JsonMap2listFilterPlugin",
  File.expand_path('../../../../classpath', __FILE__))

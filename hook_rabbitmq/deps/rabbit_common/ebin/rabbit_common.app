{application, 'rabbit_common', [
	{description, "Modules shared by rabbitmq-server and rabbitmq-erlang-client"},
	{vsn, "4.3.4"},
	{id, "v4.3.4"},
	{modules, ['app_utils','code_version','credit_flow','delegate','delegate_sup','gen_server2','mirrored_supervisor_locks','pmon','priority_queue','rabbit_amqp_connection','rabbit_amqqueue_common','rabbit_auth_mechanism','rabbit_basic_common','rabbit_binary_generator','rabbit_binary_parser','rabbit_cert_info','rabbit_channel_common','rabbit_command_assembler','rabbit_control_misc','rabbit_core_metrics','rabbit_data_coercion','rabbit_date_time','rabbit_env','rabbit_error_logger_handler','rabbit_event','rabbit_framing','rabbit_framing_amqp_0_9_1','rabbit_heartbeat','rabbit_http_util','rabbit_json','rabbit_log','rabbit_misc','rabbit_net','rabbit_nodes_common','rabbit_numerical','rabbit_password','rabbit_password_hashing','rabbit_password_hashing_md5','rabbit_password_hashing_sha256','rabbit_password_hashing_sha512','rabbit_pbe','rabbit_peer_discovery_backend','rabbit_policy_validator','rabbit_queue_collector','rabbit_re','rabbit_registry','rabbit_registry_class','rabbit_resource_monitor_misc','rabbit_routing_parser','rabbit_runtime','rabbit_runtime_parameter','rabbit_semver','rabbit_semver_parser','rabbit_ssl_options','rabbit_term_decoding','rabbit_types','rabbit_writer','supervisor2','worker_pool','worker_pool_sup','worker_pool_worker']},
	{registered, []},
	{applications, [kernel,stdlib,compiler,crypto,public_key,sasl,ssl,syntax_tools,tools,runtime_tools,thoas,ranch,recon,credentials_obfuscation]},
	{optional_applications, []},
	{env, []},
	%% Hex.pm package informations.
	{licenses, ["MPL-2.0"]},
	{links, [
	    {"Website", "https://www.rabbitmq.com/"},
	    {"GitHub", "https://github.com/rabbitmq/rabbitmq-server/tree/main/deps/rabbit_common"}
	  ]},
	{build_tools, ["make", "rebar3"]},
	{files, [
	    	    "erlang.mk",
	    "git-revisions.txt",
	    "include",
	    "LICENSE*",
	    "Makefile",
	    "rabbitmq-components.mk",
	    "README",
	    "README.md",
	    "src",
	    "mk"
	  ]}
]}.
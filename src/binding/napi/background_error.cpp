#include "napi/background_error.h"
#include "core/debug.h"
#include "napi/macros.h"

namespace rocksdb_js {

AddonData* getAddonData(napi_env env) {
	void* data = nullptr;
	if (::napi_get_instance_data(env, &data) != napi_ok) {
		return nullptr;
	}
	return static_cast<AddonData*>(data);
}

napi_value BackgroundError::Constructor(napi_env env, napi_callback_info info) {
	NAPI_CONSTRUCTOR_ARGV("BackgroundError", 1);

	napi_value global;
	NAPI_STATUS_THROWS(::napi_get_global(env, &global));

	// Copy the serialized fields (message, severity, severityName, writesDisabled,
	// reason?, reasonName?, type) onto the instance. Object.assign is used so the
	// set stays in step with backgroundErrorToJson without re-listing every key.
	if (argc >= 1) {
		napi_valuetype argType;
		NAPI_STATUS_THROWS(::napi_typeof(env, argv[0], &argType));
		if (argType == napi_object) {
			napi_value objectCtor;
			napi_value assign;
			napi_value assignResult;
			NAPI_STATUS_THROWS(::napi_get_named_property(env, global, "Object", &objectCtor));
			NAPI_STATUS_THROWS(::napi_get_named_property(env, objectCtor, "assign", &assign));
			napi_value assignArgs[2] = { jsThis, argv[0] };
			NAPI_STATUS_THROWS(::napi_call_function(env, objectCtor, assign, 2, assignArgs, &assignResult));
		}
	}

	// name is fixed (details carries `type`, not `name`), so set it after the copy.
	napi_value name;
	NAPI_STATUS_THROWS(::napi_create_string_utf8(env, "BackgroundError", NAPI_AUTO_LENGTH, &name));
	NAPI_STATUS_THROWS(::napi_set_named_property(env, jsThis, "name", name));

	// No stack capture: a BackgroundError originates on a RocksDB background
	// thread, so a JS stack points at whoever happened to read it, not the cause
	// — meaningless, and Error.captureStackTrace is not free.

	return jsThis;
}

void BackgroundError::Init(napi_env env, napi_value exports) {
	napi_value ctor;
	NAPI_STATUS_THROWS_VOID(::napi_define_class(
		env,
		"BackgroundError",
		NAPI_AUTO_LENGTH,
		BackgroundError::Constructor,
		nullptr,
		0,
		nullptr,
		&ctor
	));

	// Make it a real Error subclass: BackgroundError.prototype ->> Error.prototype
	// and BackgroundError ->> Error (static inheritance). instanceof Error then
	// holds, and Error.prototype.toString formats name + message.
	napi_value global;
	napi_value objectCtor;
	napi_value setPrototypeOf;
	napi_value errorCtor;
	napi_value errorProto;
	napi_value ctorProto;
	NAPI_STATUS_THROWS_VOID(::napi_get_global(env, &global));
	NAPI_STATUS_THROWS_VOID(::napi_get_named_property(env, global, "Object", &objectCtor));
	NAPI_STATUS_THROWS_VOID(::napi_get_named_property(env, objectCtor, "setPrototypeOf", &setPrototypeOf));
	NAPI_STATUS_THROWS_VOID(::napi_get_named_property(env, global, "Error", &errorCtor));
	NAPI_STATUS_THROWS_VOID(::napi_get_named_property(env, errorCtor, "prototype", &errorProto));
	NAPI_STATUS_THROWS_VOID(::napi_get_named_property(env, ctor, "prototype", &ctorProto));

	napi_value protoArgs[2] = { ctorProto, errorProto };
	napi_value protoResult;
	NAPI_STATUS_THROWS_VOID(::napi_call_function(env, objectCtor, setPrototypeOf, 2, protoArgs, &protoResult));
	napi_value staticArgs[2] = { ctor, errorCtor };
	NAPI_STATUS_THROWS_VOID(::napi_call_function(env, objectCtor, setPrototypeOf, 2, staticArgs, &protoResult));

	// Stash the constructor per-env so New() can build instances from anywhere
	// holding just the env (Database methods and the event-emitter trampoline).
	// Create the reference before allocating AddonData so a failed create leaves
	// nothing to leak.
	napi_ref ctorRef = nullptr;
	NAPI_STATUS_THROWS_VOID(::napi_create_reference(env, ctor, 1, &ctorRef));
	auto* addon = new AddonData();
	addon->backgroundErrorCtor = ctorRef;
	napi_status dataStatus = ::napi_set_instance_data(
		env,
		addon,
		[](napi_env env, void* data, void* /*hint*/) {
			auto* addon = static_cast<AddonData*>(data);
			if (addon == nullptr) {
				return;
			}
			if (addon->backgroundErrorCtor != nullptr) {
				::napi_delete_reference(env, addon->backgroundErrorCtor);
			}
			delete addon;
		},
		nullptr
	);
	if (dataStatus != napi_ok) {
		::napi_delete_reference(env, addon->backgroundErrorCtor);
		delete addon;
		std::string errorStr = rocksdb_js::getNapiExtendedError(env, dataStatus, "Failed to set addon instance data");
		::napi_throw_error(env, nullptr, errorStr.c_str());
		return;
	}

	NAPI_STATUS_THROWS_VOID(::napi_set_named_property(env, exports, "BackgroundError", ctor));
}

napi_value BackgroundError::New(napi_env env, const std::string& json) {
	AddonData* addon = getAddonData(env);
	if (addon == nullptr || addon->backgroundErrorCtor == nullptr) {
		::napi_throw_error(env, nullptr, "BackgroundError constructor is not initialized for this env");
		return nullptr;
	}

	napi_value ctor;
	NAPI_STATUS_THROWS(::napi_get_reference_value(env, addon->backgroundErrorCtor, &ctor));

	napi_value global;
	napi_value jsonObj;
	napi_value parse;
	napi_value jsonString;
	napi_value details;
	NAPI_STATUS_THROWS(::napi_get_global(env, &global));
	NAPI_STATUS_THROWS(::napi_get_named_property(env, global, "JSON", &jsonObj));
	NAPI_STATUS_THROWS(::napi_get_named_property(env, jsonObj, "parse", &parse));
	NAPI_STATUS_THROWS(::napi_create_string_utf8(env, json.c_str(), json.length(), &jsonString));
	NAPI_STATUS_THROWS(::napi_call_function(env, jsonObj, parse, 1, &jsonString, &details));

	napi_value instance;
	NAPI_STATUS_THROWS(::napi_new_instance(env, ctor, 1, &details, &instance));
	return instance;
}

} // namespace rocksdb_js

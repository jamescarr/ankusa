export { createAdminClient } from "./client.js";
export type {
  AdminClient,
  AdminClientOptions,
  Health,
  DlqPage,
  QuarantinePage,
  Replayed,
  ReplayFilter,
  ListDeadLettersParams,
  ListQuarantinedParams,
} from "./client.js";
export {
  AdminError,
  AdminUnavailableError,
  RoleNotEnabledError,
  AdminRejectedError,
} from "./errors.js";

import { z } from 'zod';
import { userSchema } from '../auth/api';
export const adminUserSchema = userSchema.extend({
  disabledAt: z.string().nullable(),
  createdAt: z.string(),
  updatedAt: z.string(),
  revision: z.number().int().nonnegative(),
});
export type AdminUser = z.infer<typeof adminUserSchema>;
const page = {
  total: z.number().int().nonnegative(),
  page: z.number().int().positive(),
  pageSize: z.number().int().positive(),
};
export const usersSchema = z.object({
  ...page,
  items: z.array(adminUserSchema),
  activeAdminCount: z.number().int().nonnegative(),
});
export const invitationSchema = z.object({
  id: z.string(),
  createdAt: z.string(),
  expiresAt: z.string(),
  createdBy: z.string(),
  usedBy: z.string().nullable(),
  usedAt: z.string().nullable(),
  revokedAt: z.string().nullable(),
  status: z.enum(['available', 'used', 'expired', 'revoked']),
});
export type Invitation = z.infer<typeof invitationSchema>;
export const invitesSchema = z.object({
  ...page,
  items: z.array(invitationSchema),
});
export const auditSchema = z.object({
  ...page,
  items: z.array(
    z.object({
      id: z.string(),
      action: z.string(),
      target: z.string().nullable(),
      detail: z.unknown(),
      createdAt: z.string(),
      actorEmail: z.string().nullable(),
      targetEmail: z.string().nullable().optional(),
    }),
  ),
});
export const mutationSchema = z.object({
  user: adminUserSchema,
  signedOut: z.boolean(),
  newPassword: z.string().optional(),
});
export type MutationResult = z.infer<typeof mutationSchema>;
export const actionNames: Record<string, string> = {
  'instrument.add': '添加采集标的',
  'instrument.edit': '编辑标的',
  'request.approve': '批准收录申请',
  'request.reject': '驳回收录申请',
  'job.dispatch': '触发采集执行器',
  'job.retry': '重试采集任务',
  'admin.bootstrap': '初始化管理员',
  'invite.create': '生成邀请码',
  'invite.revoke': '撤销邀请码',
  'user.disable': '禁用账号',
  'user.enable': '启用账号',
  'user.role': '修改角色',
  'user.password_reset': '重置密码',
};

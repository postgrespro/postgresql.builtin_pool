/* SessionVariable(type,name,init) */
SessionVariable(Oid, AuthenticatedUserId, InvalidOid)
SessionVariable(Oid, SessionUserId, InvalidOid)
SessionVariable(Oid, OuterUserId, InvalidOid)
SessionVariable(Oid, CurrentUserId, InvalidOid)
SessionVariable(bool, AuthenticatedUserIsSuperuser, false)
SessionVariable(bool, SessionUserIsSuperuser, false)
SessionVariable(int, SecurityRestrictionContext, 0)
SessionVariable(bool, SetRoleIsActive, false)
SessionVariable(Oid, last_roleid, InvalidOid)
SessionVariable(bool, last_roleid_is_super, false)
SessionVariable(struct SeqTableData*, last_used_seq, NULL)
#undef SessionVariable

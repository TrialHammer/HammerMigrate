"""Account merges whose plugin semantics differ from generic row updates.

Only explicit --merge requests can retire singleton rows. Particle groups retain
both stable UUIDs and all child references, even when their saved names collide.
"""


def merge_operations(db, identity, meta, operations, rows, location):
    table = meta["name"]
    supported = {
        "playerpoints_points": "uuid",
        "playerpoints_username_cache": "uuid",
        "playerparticles_settings": "player_uuid",
        "playerparticles_group": "owner_uuid",
        "authme": "uuid",
    }
    semantic = meta.get("semanticTable", table).lower()
    column = supported.get(semantic)
    if (
        not identity.merge
        or not column
        or not any(column == c["name"] for c in meta["columns"])
    ):
        return operations
    if not meta["pk"]:
        raise ValueError("Plugin merge requires a primary key: " + location)
    if semantic in ("authme", "playerpoints_username_cache"):
        for row in rows:
            names = [v for k, v in row.items() if k.lower() in ("username", "realname")]
            if any(
                isinstance(v, str) and v.lower() == identity.new.lower() for v in names
            ):
                if identity.raw_uuid(row.get(column)) not in (
                    identity.raw_old,
                    identity.raw_new,
                ):
                    raise ValueError("Target name refers to another UUID: " + location)
    source = [r for r in rows if identity.raw_uuid(r.get(column)) == identity.raw_old]
    target = [r for r in rows if identity.raw_uuid(r.get(column)) == identity.raw_new]
    if semantic == "playerparticles_group":
        occupied = {r["name"].casefold() for r in source + target}
        for old in source:
            for current in target:
                if old["name"].casefold() != current["name"].casefold():
                    continue
                base = "recovery-target-" + current["name"]
                name, number = base, 1
                while name.casefold() in occupied:
                    name = base + "-" + str(number)
                    number += 1
                if len(name) > 100:
                    raise ValueError("Particle group recovery name is too long")
                occupied.add(name.casefold())
                operations.append(
                    dict(
                        table=table,
                        pk=meta["pk"],
                        before=current,
                        after=dict(current, name=name),
                        method="preserve-target-group",
                    )
                )
    else:
        if len(source) > 1 or len(target) > 1:
            raise ValueError("Ambiguous singleton plugin account: " + location)
        if source and target:
            source_op = next(o for o in operations if o["before"] == source[0])
            if semantic == "playerpoints_points":
                total = source[0]["points"] + target[0]["points"]
                if not -(2**31) <= total < 2**31:
                    raise ValueError(
                        "PlayerPoints merged balance exceeds signed integer range"
                    )
                source_op["after"]["points"] = total
                source_op["method"] = "sum-playerpoints-balances"
            operations.insert(
                0,
                dict(
                    table=table,
                    pk=meta["pk"],
                    before=target[0],
                    after=None,
                    method="retire-merged-account",
                ),
            )
    # Every destination identity in a supported singleton/group table has now
    # been explicitly retained or handled. Unrecognized columns still block.
    identity.collisions[:] = [
        c
        for c in identity.collisions
        if not (
            c["location"] == location + "." + column
            or semantic in ("authme", "playerpoints_username_cache")
            and c["location"].rsplit(".", 1)[0] == location
            and c["location"].rsplit(".", 1)[1].lower() in ("username", "realname")
        )
    ]
    return operations

"""Validate premium login against the currently installed authentication topology."""


def premium_errors(login, auth, proxy_cache=False):
    errors = []
    backends = [v for v in login if v['type'] == 'authme' and v.get('installed', True)]
    proxies = [v for v in login if v['type'] == 'authme-bungee' and v.get('installed', True)]
    if not auth or not backends or not all(v['enabled'] for v in backends):
        errors.append({'error': 'Premium login requires an AuthMe binding and enabled backend verification'})
    for backend in backends:
        if backend.get('proxyForwarding', False):
            if not proxies or not all(v['keepOfflineUuidCompatibility'] for v in proxies):
                errors.append({'error': 'Premium login requires offline UUID compatibility on every proxy'})
            if not proxy_cache:
                errors.append({'error': 'Premium cache was not discovered'})
        elif backend.get('onlineMode', False):
            errors.append({'error': 'Offline UUID migration requires offline-mode authentication', 'path': backend['path']})
        elif backend.get('proxyHook', False) or not backend.get('packetEvents', True):
            errors.append({'error': 'Direct premium login requires Hooks.bungeecord=false and PacketEvents', 'path': backend['path']})
    return errors

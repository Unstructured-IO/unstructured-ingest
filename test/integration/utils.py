import os

import pytest


def requires_env(*envs):
    if len(envs) == 1:
        env = envs[0]
        return pytest.mark.skipif(
            env not in os.environ, reason=f"Environment variable not set: {env}"
        )
    return pytest.mark.skipif(
        not all(env in os.environ for env in envs),
        reason="All required environment variables not set: {}".format(", ".join(envs)),
    )

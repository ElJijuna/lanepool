# [1.1.0](https://github.com/ElJijuna/lanepool/compare/v1.0.0...v1.1.0) (2026-09-22)


### Features

* add concurrencyKey and concurrencyLimit options for job processing ([3e9a76a](https://github.com/ElJijuna/lanepool/commit/3e9a76a3a994f76cd71a044a8d44d9a13f6e609e))

# 1.0.0 (2026-09-22)


### Features

* add event system for job lifecycle and error handling with typed events ([6990a76](https://github.com/ElJijuna/lanepool/commit/6990a76e5492282c7d1960994c88b1b353aab50b))
* add scheduled retries handling and test for retry backoff delay in queue processing ([072048d](https://github.com/ElJijuna/lanepool/commit/072048de36782447cd5453be24c927e685c77d0e))
* add timeout and retry delay options for job processing with error handling ([4b5ed86](https://github.com/ElJijuna/lanepool/commit/4b5ed86eed05f74194feb028e744efee3922ef4e))
* add workflow support with job dependencies and state management ([2c23ded](https://github.com/ElJijuna/lanepool/commit/2c23ded14f6eaa8a948cdb9394f042d0f00d32eb))
* implement job retry mechanism with maxAttempts for queue processing ([bd454fa](https://github.com/ElJijuna/lanepool/commit/bd454fa0db5355399830e6299a474fe1796e4938))
* initialize lanepool package with in-memory processing queue ([31a6487](https://github.com/ElJijuna/lanepool/commit/31a6487da5d98d7e0ed1603f731496e54756e0f0))

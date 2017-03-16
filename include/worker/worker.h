/**
 * @file worker.h
 *
 * @brief Header of worker node management.
 */

#ifndef USTORE_WORKER_H_
#define USTORE_WORKER_H_

#include "spec/value.h"
#include "utils/noncopyable.h"

namespace ustore {

/* Management related type alias */
using WorkerID = uint32_t;
using ErrorCode = int;

/* Data related type alias */
using DataValue = Value;
using DataKey = uint64_t;
using DataVersion = uint64_t;

using Branch = std::string;
using ChunkHash = uint64_t;

/**
 * @brief Worker node management.
 */
class Worker : public Noncopyable {
  public:
	const WorkerID id;

	Worker();
	~Worker();

	/**
	 * @brief Read data.
	 *
	 * @return Value of data.
	 */
	DataValue Get() const;

	/**
	 * @brief Write data with its specified version and branch.
	 *
	 * @param ver Data version.
	 * @param branch The operating branch.
	 * @return Error code. (0 for success)
	 */
	ErrorCode Put(const DataVersion& ver, const Branch& branch);

	/**
	 * @brief Create a new branch for the data.
	 *
	 * @param bash_branch The base branch.
	 * @param new_branch The new branch.
	 * @return Error code. (0 for success)
	 */
	ErrorCode Fork(const Branch& base_branch, const Branch& new_branch);

	/**
	 * @brief Move data to another branch.
	 *
	 * @param from_branch The original branch.
	 * @param to_branch The target branch.
	 * @return Error code. (0 for success)
	 */
	ErrorCode Move(const Branch& from_branch, const Branch& to_branch);

	/**
	 * @brief Merge two branches of the data.
	 *
	 * @param op_branch The operating branch.
	 * @param ref_branch The referring branch.
	 * @return Error code. (0 for success)
	 */
	ErrorCode Merge(const Branch& op_branch, const Branch& ref_branch);

  private:
	const HeadVersion head_ver;
};

/**
 * @brief Table of head versions of data.
 *
 * This class should only be instantiated by Worker.
 */
class HeadVersion {
  public:
	HeadVersion();
	~HeadVersion();

	/**
	 * @brief Retrieve the head version of data according to the specified
	 *        branch.
	 *
	 * @param key Data key.
	 * @param branch Branch to look for.
	 * @return Head version of data as per request.
	 */
	const DataVersion Get(const DataKey& key, const Branch& branch);

	/**
	 * @brief Update the head version of data according to the specified
	 *        branch.
	 *
	 * @param ver The updating version.
	 * @param key Data key.
	 * @param branch Branch to update.
	 * @return Error code. (0 for success)
	 */
	ErrorCode Put(const DataVersion& ver, const DataKey& key, const Branch& branch);

  private:
};

} // namespace ustore

#endif /* USTORE_WORKER_H_ */

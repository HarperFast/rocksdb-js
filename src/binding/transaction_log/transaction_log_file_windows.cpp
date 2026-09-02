#include "transaction_log_file.h"

#ifdef PLATFORM_WINDOWS

#include "core/debug.h"
#include "core/encoding.h"
#include "core/platform.h"
#include <aclapi.h>
#include <sddl.h>
#include <algorithm>
#include <cstdio>
#include <vector>

namespace rocksdb_js {

std::string getWindowsErrorMessage(DWORD errorCode);
static std::atomic<uint64_t> appendBoundaryTempSequence{0};

TransactionLogFile::TransactionLogFile(
	const std::filesystem::path& p,
	const uint32_t seq,
	bool appendBoundaryMarkerEnabled) :
	path(p),
	sequenceNumber(seq),
	appendBoundaryMarkerEnabled(appendBoundaryMarkerEnabled)
{}

void TransactionLogFile::ensureAppendBoundaryMarker() {
	auto markerPath = transactionLogAppendBoundaryMarkerPath(this->path);
	if (this->readOnly) {
		// Read an existing marker (its boundary caps what this reader may
		// expose) but never create, repair, or remove one — the marker tree
		// belongs to the writer, which may be live in another process.
		std::error_code readOnlyExistsError;
		if (std::filesystem::exists(markerPath, readOnlyExistsError)) {
			try {
				this->retiredAppendBoundary.store(
					readTransactionLogAppendBoundaryMarker(this->path), std::memory_order_relaxed);
			} catch (const TransactionLogAppendBoundaryException&) {
				if (std::filesystem::exists(this->path)) {
					throw;
				}
			}
		}
		return;
	}
	std::error_code existsError;
	if (std::filesystem::exists(markerPath, existsError)) {
		try {
			uint32_t boundary = readTransactionLogAppendBoundaryMarker(this->path);
			if (boundary == 0 || std::filesystem::exists(this->path)) {
				this->retiredAppendBoundary.store(boundary, std::memory_order_relaxed);
				return;
			}
		} catch (const TransactionLogAppendBoundaryException&) {
			if (std::filesystem::exists(this->path)) {
				throw;
			}
		}
		std::error_code removeError;
		if (!std::filesystem::remove(markerPath, removeError) || removeError) {
			throw rocksdb_js::TransactionLogAppendBoundaryException(
				"Failed to remove stale transaction log append-boundary marker: " +
				markerPath.string());
		}
	}
	if (existsError) {
		throw rocksdb_js::TransactionLogAppendBoundaryException(
			"Failed to inspect transaction log append-boundary marker: " + markerPath.string());
	}

	rocksdb_js::tryCreateDirectory(markerPath.parent_path());
	auto tempMarkerPath = markerPath;
	std::wstring tempSuffix = L".tmp-" + std::to_wstring(::GetCurrentProcessId()) + L"-" +
		std::to_wstring(appendBoundaryTempSequence.fetch_add(1, std::memory_order_relaxed));
	tempMarkerPath += tempSuffix;
	HANDLE marker = ::CreateFileW(
		tempMarkerPath.wstring().c_str(),
		GENERIC_WRITE,
		FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
		nullptr,
		CREATE_NEW,
		FILE_ATTRIBUTE_HIDDEN,
		nullptr);
	if (marker == INVALID_HANDLE_VALUE) {
		throw rocksdb_js::TransactionLogAppendBoundaryException(
			"Failed to create temporary transaction log append-boundary marker: " +
			tempMarkerPath.string());
	}

	char bytes[TRANSACTION_LOG_APPEND_BOUNDARY_MARKER_SIZE];
	writeUint32BE(bytes, TRANSACTION_LOG_APPEND_BOUNDARY_MARKER_TOKEN);
	writeUint32BE(bytes + 4, 0);
	writeUint32BE(bytes + 8, UINT32_MAX);
	DWORD written = 0;
	bool success = ::WriteFile(marker, bytes, sizeof(bytes), &written, nullptr) &&
		written == static_cast<DWORD>(sizeof(bytes)) && ::FlushFileBuffers(marker);
	::CloseHandle(marker);
	if (!success) {
		std::filesystem::remove(tempMarkerPath);
		throw rocksdb_js::TransactionLogAppendBoundaryException(
			"Failed to initialize transaction log append-boundary marker: " + markerPath.string());
	}
	if (!::MoveFileExW(
			tempMarkerPath.wstring().c_str(), markerPath.wstring().c_str(), MOVEFILE_WRITE_THROUGH)) {
		DWORD publishError = ::GetLastError();
		std::filesystem::remove(tempMarkerPath);
		if (publishError == ERROR_FILE_EXISTS || publishError == ERROR_ALREADY_EXISTS) {
			this->retiredAppendBoundary.store(
				readTransactionLogAppendBoundaryMarker(this->path), std::memory_order_relaxed);
			return;
		}
		throw rocksdb_js::TransactionLogAppendBoundaryException(
			"Failed to publish transaction log append-boundary marker: " + markerPath.string());
	}
}

void TransactionLogFile::writeAppendBoundaryMarker(uint32_t boundary) {
	auto markerPath = transactionLogAppendBoundaryMarkerPath(this->path);
	HANDLE marker = ::CreateFileW(
		markerPath.wstring().c_str(),
		GENERIC_WRITE,
		FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
		nullptr,
		OPEN_EXISTING,
		FILE_ATTRIBUTE_HIDDEN,
		nullptr);
	if (marker == INVALID_HANDLE_VALUE) {
		throw rocksdb_js::TransactionLogAppendBoundaryException(
			"Failed to open transaction log append-boundary marker: " + markerPath.string());
	}
	char bytes[TRANSACTION_LOG_APPEND_BOUNDARY_MARKER_SIZE];
	writeUint32BE(bytes, TRANSACTION_LOG_APPEND_BOUNDARY_MARKER_TOKEN);
	writeUint32BE(bytes + 4, boundary);
	writeUint32BE(bytes + 8, ~boundary);
	DWORD written = 0;
	bool success = ::WriteFile(marker, bytes, sizeof(bytes), &written, nullptr) &&
		written == static_cast<DWORD>(sizeof(bytes)) && ::FlushFileBuffers(marker);
	::CloseHandle(marker);
	if (!success) {
		throw rocksdb_js::TransactionLogAppendBoundaryException(
			"Failed to persist transaction log append boundary: " + markerPath.string());
	}
}

void TransactionLogFile::close() {
	std::lock_guard<std::mutex> lock(this->fileMutex);
	this->closeLocked();
}

void TransactionLogFile::closeLocked() {
	// Explicitly remove our reference to the memory map.
	if (this->memoryMap) {
		DEBUG_LOG("%p TransactionLogFile::close Closing memory map for: %s (ref count=%ld)\n",
			this, this->path.string().c_str(), this->memoryMap.use_count());
		this->memoryMap.reset();
	}

	if (this->fileHandle != INVALID_HANDLE_VALUE) {
		DEBUG_LOG("%p TransactionLogFile::close Closing file: %s (handle=%p)\n",
			this, this->path.string().c_str(), this->fileHandle);
		::CloseHandle(this->fileHandle);
		this->fileHandle = INVALID_HANDLE_VALUE;
	}
}

void TransactionLogFile::flush() {
	std::unique_lock<std::mutex> lock(this->fileMutex);
	uint32_t currentSize = this->size.load(std::memory_order_relaxed);
	// Only flush if there's new data since the last flush
	if (this->fileHandle == INVALID_HANDLE_VALUE || currentSize <= this->lastFlushedSize) {
		return; // return early
	}
	// Perform the flush without holding the lock (since fdatasync/fsync can be slow)
	HANDLE handleToFlush = this->fileHandle;
	lock.unlock();

	// Perform the flush without holding the lock (since FlushFileBuffers can be slow)
	DEBUG_LOG("%p TransactionLogFile::flush Flushing file: %s (handle=%p, size=%u, lastFlushedSize=%u)\n",
		this, this->path.string().c_str(), handleToFlush, currentSize, this->lastFlushedSize);
	if (!::FlushFileBuffers(handleToFlush)) {
		DWORD error = ::GetLastError();
		std::string errorMessage = getWindowsErrorMessage(error);
		DEBUG_LOG("%p TransactionLogFile::flush ERROR: FlushFileBuffers failed: %s (error=%lu: %s)\n",
			this, this->path.string().c_str(), error, errorMessage.c_str());
		throw rocksdb_js::DBException("Failed to flush file: " + this->path.string());
	}

	// Update the last flushed size after successful sync
	lock.lock();
	this->lastFlushedSize = currentSize;
}

void TransactionLogFile::openFile() {
	if (this->fileHandle != INVALID_HANDLE_VALUE) {
		DEBUG_LOG("%p TransactionLogFile::openFile File already open: %s\n", this, this->path.string().c_str());
		return;
	}

	// Fresh (re)open: until the first append, a zero timestamp seen while indexing is a genuine
	// end-of-data marker (and this->size may be seeded from a padded on-disk size that needs
	// correcting), so findPositionByTimestamp is allowed to correct this->size. See hasAppendedSinceOpen.
	this->hasAppendedSinceOpen.store(false);

	DEBUG_LOG("%p TransactionLogFile::openFile Opening file: %s\n", this, this->path.string().c_str());

	// ensure parent directory exists (may have been deleted by purge())
	auto parentPath = this->path.parent_path();
	if (!parentPath.empty()) {
		try {
			DEBUG_LOG("%p TransactionLogFile::openFile Creating parent directory: %s\n", this, parentPath.string().c_str());
			rocksdb_js::tryCreateDirectory(parentPath);
		} catch (const std::filesystem::filesystem_error& e) {
			DEBUG_LOG("%p TransactionLogFile::openFile Failed to create parent directory: %s (error=%s)\n",
				this, parentPath.string().c_str(), e.what());
			throw rocksdb_js::DBException("Failed to create parent directory: " + parentPath.string());
		}
	}

	// Check if file already exists before creating/opening
	bool fileExisted = std::filesystem::exists(this->path);

	if (this->readOnly) {
		// A reader never creates the file and can open logs on a read-only or
		// otherwise write-protected volume.
		this->fileHandle = ::CreateFileW(
			this->path.wstring().c_str(),
			GENERIC_READ,
			FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
			nullptr,
			OPEN_EXISTING,
			FILE_ATTRIBUTE_NORMAL,
			nullptr
		);
		if (this->fileHandle == INVALID_HANDLE_VALUE) {
			DWORD error = ::GetLastError();
			std::string errorMessage = getWindowsErrorMessage(error);
			DEBUG_LOG("%p TransactionLogFile::openFile Failed to open sequence file for read: %s (error=%lu: %s)\n",
				this, this->path.string().c_str(), error, errorMessage.c_str());
			throw rocksdb_js::DBException("Failed to open sequence file for read: " + this->path.string());
		}
	} else {
		// open file for both reading and writing
		this->fileHandle = ::CreateFileW(
			this->path.wstring().c_str(),
			GENERIC_READ | GENERIC_WRITE,
			FILE_SHARE_READ | FILE_SHARE_WRITE,
			nullptr,
			OPEN_ALWAYS,
			FILE_ATTRIBUTE_NORMAL,
			nullptr
		);

		if (this->fileHandle == INVALID_HANDLE_VALUE) {
			DWORD error = ::GetLastError();
			std::string errorMessage = getWindowsErrorMessage(error);
			DEBUG_LOG("%p TransactionLogFile::openFile Failed to open sequence file for read/write: %s (error=%lu: %s)\n",
				this, this->path.string().c_str(), error, errorMessage.c_str());
			throw rocksdb_js::DBException("Failed to open sequence file for read/write: " + this->path.string());
		}
	}

	// Set file permissions equivalent to Unix 640 (owner: read+write, group: read, others: none)
	// Only set permissions if the file was just created (not if it already existed)
	if (!fileExisted) {
		PSID ownerSid = nullptr;
		PSID groupSid = nullptr;
		PACL dacl = nullptr;
		PSECURITY_DESCRIPTOR securityDescriptor = nullptr;

		// Get the file's current security descriptor
		std::wstring wpath = this->path.wstring();
		DWORD result = ::GetNamedSecurityInfoW(
			const_cast<LPWSTR>(wpath.c_str()),
			SE_FILE_OBJECT,
			OWNER_SECURITY_INFORMATION | GROUP_SECURITY_INFORMATION | DACL_SECURITY_INFORMATION,
			&ownerSid,
			&groupSid,
			&dacl,
			nullptr,
			&securityDescriptor
		);

		if (result == ERROR_SUCCESS && ownerSid && groupSid) {
			// Create a new DACL with 640 permissions
			EXPLICIT_ACCESS_W ea[2];
			ZeroMemory(ea, sizeof(ea));

			// Owner: read + write
			ea[0].grfAccessPermissions = FILE_GENERIC_READ | FILE_GENERIC_WRITE;
			ea[0].grfAccessMode = SET_ACCESS;
			ea[0].grfInheritance = NO_INHERITANCE;
			ea[0].Trustee.TrusteeForm = TRUSTEE_IS_SID;
			ea[0].Trustee.TrusteeType = TRUSTEE_IS_USER;
			ea[0].Trustee.ptstrName = reinterpret_cast<LPWSTR>(ownerSid);

			// Group: read only
			ea[1].grfAccessPermissions = FILE_GENERIC_READ;
			ea[1].grfAccessMode = SET_ACCESS;
			ea[1].grfInheritance = NO_INHERITANCE;
			ea[1].Trustee.TrusteeForm = TRUSTEE_IS_SID;
			ea[1].Trustee.TrusteeType = TRUSTEE_IS_GROUP;
			ea[1].Trustee.ptstrName = reinterpret_cast<LPWSTR>(groupSid);

			PACL newDacl = nullptr;
			result = ::SetEntriesInAclW(2, ea, dacl, &newDacl);
			if (result == ERROR_SUCCESS && newDacl) {
				// Apply the new DACL
				result = ::SetNamedSecurityInfoW(
					const_cast<LPWSTR>(wpath.c_str()),
					SE_FILE_OBJECT,
					DACL_SECURITY_INFORMATION | PROTECTED_DACL_SECURITY_INFORMATION,
					nullptr,
					nullptr,
					newDacl,
					nullptr
				);
				if (result != ERROR_SUCCESS) {
					DEBUG_LOG("%p TransactionLogFile::openFile Failed to set file permissions: %s (error=%lu)\n",
						this, this->path.string().c_str(), result);
				}
				::LocalFree(newDacl);
			}
		}

		if (securityDescriptor) {
			::LocalFree(securityDescriptor);
		}
	}

	// Get file size
	LARGE_INTEGER fileSize;
	if (!::GetFileSizeEx(this->fileHandle, &fileSize)) {
		DWORD error = ::GetLastError();
		std::string errorMessage = getWindowsErrorMessage(error);
		DEBUG_LOG("%p TransactionLogFile::openFile Failed to get file size: %s (error=%lu: %s)\n",
			this, this->path.string().c_str(), error, errorMessage.c_str());
		throw rocksdb_js::DBException("Failed to get file size: " + this->path.string());
	}
	auto size = static_cast<size_t>(fileSize.QuadPart);
	uint32_t retiredBoundary = this->retiredAppendBoundary.load(std::memory_order_relaxed);
	if (retiredBoundary > size) {
		throw rocksdb_js::TransactionLogAppendBoundaryException(
			"Transaction log append boundary exceeds physical extent: " + this->path.string());
	}
	this->size = retiredBoundary > 0 ? retiredBoundary : size;
	DEBUG_LOG("%p TransactionLogFile::openFile File size: %zu file path: %s\n",
		this, size, this->path.string().c_str());
	// On Windows, we have to create the full file size for memory maps, and it is zero-padded, so the act of indexing allows us to find
	// the end, and adjust the real size accordingly.
	// TODO: Future optimization is to only do this if the file is a multiple of the page size, and ensure
	// files that are expanded to a memory page are memory page aligned, with (this->size & 0xFFF) == 0
	if (size > 0 && retiredBoundary == 0) {
		// openFile() runs under fileMutex (held by open()); pass fileMutexHeld so
		// findPositionByTimestamp() -> getMemoryMapLocked() does not re-lock it
		// (std::mutex is not recursive — re-locking would self-deadlock/terminate).
		// isCurrent is ignored by the Windows getMemoryMapLocked() (it always
		// retains a strong reference), so the value passed here is immaterial.
		this->findPositionByTimestamp(0, size, /*isCurrent=*/true, /*fileMutexHeld=*/true);
		DEBUG_LOG("%p TransactionLogFile::openFile New file size: %zu file path: %s\n",
			this, size, this->path.string().c_str());
	}
}

// Precondition: caller holds fileMutex (the guard for this->memoryMap /
// this->fileHandle). The public getMemoryMap() wrapper acquires it; the open path
// holds it already (and reaches here via findPositionByTimestamp(fileMutexHeld=true),
// which calls getMemoryMapLocked() directly rather than re-acquiring fileMutex).
//
// On Windows the weak-for-frozen ownership optimization (POSIX) is not applied:
// Windows is not Harper's memory-pressure target and uses a different mapping
// model (the file is pre-extended to maxFileSize). This function never consults
// frozenMapCache and always stores the mapping strongly in this->memoryMap, so
// `isCurrent` is ignored. (downgradeMapToFrozen() is shared code and still resets
// this->memoryMap on rotation, but a later frozen read here simply re-creates the
// mapping and re-pins it strongly for the rest of the file's life — so frozen maps
// are neither weak-held nor deduped on Windows, by design.)
std::shared_ptr<MemoryMap> TransactionLogFile::getMemoryMapLocked(uint32_t fileSize, bool isCurrent) {
	(void)isCurrent;
	// CreateFileMappingW and MapViewOfFile with length 0 may have undefined behavior.
	// Different runtimes handle this differently - Node.js/Bun tolerate it,
	// but Deno stalls. Return nullptr for empty files.
	if (fileSize == 0) {
		DEBUG_LOG("%p TransactionLogFile::getMemoryMapLocked fileSize is 0, returning nullptr\n", this);
		return nullptr;
	}

	if (this->fileHandle == INVALID_HANDLE_VALUE) {
		DEBUG_LOG("%p TransactionLogFile::getMemoryMap file is not open: %s\n", this, this->path.string().c_str());
		return nullptr;
	}

	if (this->memoryMap) {
		if (this->memoryMap->mapSize >= fileSize) {
			// existing memory map will work
			DEBUG_LOG("%p TransactionLogFile::getMemoryMap Returning existing memory map (map size=%u)\n", this, memoryMap->mapSize);
			this->memoryMap->fileSize = fileSize;
			return this->memoryMap;
		} else {
			DEBUG_LOG("%p TransactionLogFile::getMemoryMap Existing memory map was too small, creating new map (map size=%u)\n", this, memoryMap->mapSize);
		}
		// this memory map is not big enough, need to create a new one
	} else {
		DEBUG_LOG("%p TransactionLogFile::getMemoryMap Creating new memory map: %u\n", this, fileSize);
	}

	// In windows, we can not map beyond the size of the file (without using driver-level APIs that directly call procedures
	// in NT.DLL). So we must expand the file to the full size before we can map it.
	// Check the actual file size on disk to avoid repeated expansions
	if (this->readOnly) {
		// A reader cannot (and must not) extend the file; clamp the mapping to
		// the physical size instead — for a live writer's active segment that is
		// already the writer's pre-extended maxFileSize.
		LARGE_INTEGER physicalSize;
		if (::GetFileSizeEx(this->fileHandle, &physicalSize) &&
			physicalSize.QuadPart < static_cast<LONGLONG>(fileSize)
		) {
			fileSize = static_cast<uint32_t>(physicalSize.QuadPart);
		}
		if (fileSize == 0) {
			return nullptr;
		}
	} else if (fileSize > this->size.load(std::memory_order_relaxed)) {
		LARGE_INTEGER currentPos;
		LARGE_INTEGER distanceToMove;
		// First, we have to get the current position, so we can restore it (if we get to a point where no other code relies on position, could remove this)
		distanceToMove.QuadPart = 0; // We want to move 0 bytes to query current position
		if (!::SetFilePointerEx(this->fileHandle, distanceToMove, &currentPos, FILE_CURRENT)) {
			DWORD error = ::GetLastError();
			std::string errorMessage = getWindowsErrorMessage(error);
			DEBUG_LOG("%p TransactionLogFile::getMemoryMap ERROR: Failed to SetFilePointerEx: %s (error=%lu: %s)\n",
				this, this->path.string().c_str(), error, errorMessage.c_str());
			return nullptr;
		}

		// Move to the new file size
		LARGE_INTEGER newSize;
		newSize.QuadPart = fileSize;
		if (!::SetFilePointerEx(this->fileHandle, newSize, NULL, FILE_BEGIN)) {
			DWORD error = ::GetLastError();
			std::string errorMessage = getWindowsErrorMessage(error);
			DEBUG_LOG("%p TransactionLogFile::getMemoryMap ERROR: Failed to SetFilePointerEx to new size: %s (error=%lu: %s)\n",
				this, this->path.string().c_str(), error, errorMessage.c_str());
			return nullptr;
		}

		// Set the End of File with the new file size
		if (!::SetEndOfFile(this->fileHandle)) {
			DWORD error = ::GetLastError();
			std::string errorMessage = getWindowsErrorMessage(error);
			DEBUG_LOG("%p TransactionLogFile::getMemoryMap ERROR: Failed to SetEndOfFile: %s (error=%lu: %s)\n",
				this, this->path.string().c_str(), error, errorMessage.c_str());
			return nullptr;
		}

		// Restore original position
		if (!::SetFilePointerEx(this->fileHandle, currentPos, NULL, FILE_BEGIN)) {
			DWORD error = ::GetLastError();
			std::string errorMessage = getWindowsErrorMessage(error);
			DEBUG_LOG("%p TransactionLogFile::getMemoryMap ERROR: Failed to restore position: %s (error=%lu: %s)\n",
				this, this->path.string().c_str(), error, errorMessage.c_str());
			return nullptr;
		}
	}

	HANDLE mh = ::CreateFileMappingW(this->fileHandle, NULL, PAGE_READONLY, 0, fileSize, NULL);
	if (!mh) {
		DWORD error = ::GetLastError();
		std::string errorMessage = getWindowsErrorMessage(error);
		DEBUG_LOG("%p TransactionLogFile::getMemoryMap ERROR: Failed to CreateFileMapping: %s (error=%lu: %s)\n",
			this, this->path.string().c_str(), error, errorMessage.c_str());
		return nullptr;
	}

	// map the memory object into our address space
	// note that MapViewOfFileEx can be used if we wanted to suggest an address
	void* map = ::MapViewOfFile(mh, FILE_MAP_READ, 0, 0, fileSize);
	if (!map) {
		DWORD error = ::GetLastError();
		std::string errorMessage = getWindowsErrorMessage(error);
		DEBUG_LOG("%p TransactionLogFile::getMemoryMap ERROR: Failed to MapViewOfFile: %s (error=%lu: %s)\n",
			this, this->path.string().c_str(), error, errorMessage.c_str());
		::CloseHandle(mh);
		return nullptr;
	}

	// Close the mapping handle immediately after mapping the view.
	// On Windows, once a view is mapped, the mapping handle can be closed - the view
	// remains valid until UnmapViewOfFile is called. Keeping the mapping handle open
	// causes Windows to synchronize writes to the file with the memory-mapped view,
	// which is extremely slow. By closing it here, writes via WriteFile will be fast.
	::CloseHandle(mh);

	DEBUG_LOG("%p TransactionLogFile::getMemoryMap Mapped to: %p\n", this, map);
	this->memoryMap = std::make_shared<MemoryMap>(map, fileSize);

	return this->memoryMap;
}

int64_t TransactionLogFile::readFromFile(void* buffer, uint32_t size, int64_t offset) {
	if (offset >= 0) {
		LARGE_INTEGER distance;
		distance.QuadPart = offset;
		if (!::SetFilePointerEx(this->fileHandle, distance, nullptr, FILE_BEGIN)) {
			return -1;
		}
	}

	DWORD bytesRead;
	bool success = ::ReadFile(this->fileHandle, buffer, size, &bytesRead, nullptr);
	return success ? static_cast<int64_t>(bytesRead) : -1;
}

bool TransactionLogFile::removeFileLocked() {
	if (this->memoryMap) {
		DEBUG_LOG("%p TransactionLogFile::removeFile Releasing memory map before removing file: %s\n",
			this, this->path.string().c_str());
		this->memoryMap.reset();
	}

	if (this->fileHandle != INVALID_HANDLE_VALUE) {
		DEBUG_LOG("%p TransactionLogFile::removeFile Closing file: %s (handle=%p)\n",
			this, this->path.string().c_str(), this->fileHandle);
		::CloseHandle(this->fileHandle);
		this->fileHandle = INVALID_HANDLE_VALUE;
	}

	DEBUG_LOG("%p TransactionLogFile::removeFile Removing file: %s\n", this, this->path.string().c_str());
	auto removed = std::filesystem::remove(this->path);
	if (!removed) {
		DEBUG_LOG("%p TransactionLogFile::removeFile File does not exist: %s\n",
			this, this->path.string().c_str());
		return false;
	}

	if (std::filesystem::exists(this->path)) {
		DEBUG_LOG("%p TransactionLogFile::removeFile File still exists: %s\n", this, this->path.string().c_str());
		return false;
	}

	DEBUG_LOG("%p TransactionLogFile::removeFile Removed file %s\n",
		this, this->path.string().c_str());
	std::error_code markerError;
	auto markerPath = transactionLogAppendBoundaryMarkerPath(this->path);
	std::filesystem::remove(markerPath, markerError);
	std::filesystem::remove(markerPath.parent_path(), markerError);
	std::filesystem::remove(markerPath.parent_path().parent_path(), markerError);
	return true;
}

int64_t TransactionLogFile::writeBatchToFile(iovec* iovecs, int iovcnt, int64_t& bytesLanded) {
	bytesLanded = 0;

	if (iovcnt <= 0) {
		return 0;
	}

	// seek to current size before writing (file pointer may have been moved by reads)
	if (::SetFilePointer(this->fileHandle, this->size.load(std::memory_order_relaxed), nullptr, FILE_BEGIN) == INVALID_SET_FILE_POINTER) {
		DWORD error = ::GetLastError();
		std::string errorMessage = getWindowsErrorMessage(error);
		DEBUG_LOG("%p TransactionLogFile::writeBatchToFile SetFilePointer failed (error=%lu: %s)\n",
			this, error, errorMessage.c_str());
		return -1;
	}

	// emulate writev() by writing each buffer sequentially. On a partial
	// WriteFile return we advance into the buffer's remainder and retry so a
	// short write does not silently drop the tail of an entry.
	int64_t totalBytesWritten = 0;

	for (int i = 0; i < iovcnt; i++) {
		char* basePtr = static_cast<char*>(iovecs[i].iov_base);
		size_t remaining = iovecs[i].iov_len;

		while (remaining > 0) {
			DWORD bytesWritten = 0;
			bool success = ::WriteFile(
				this->fileHandle,
				basePtr,
				static_cast<DWORD>(remaining),
				&bytesWritten,
				nullptr
			);

			if (!success) {
				DWORD error = ::GetLastError();
				std::string errorMessage = getWindowsErrorMessage(error);
				DEBUG_LOG("%p TransactionLogFile::writeBatchToFile WriteFile failed (error=%lu: %s, iovec %d/%d)\n",
					this, error, errorMessage.c_str(), i, iovcnt);
				// The batch seeks to `size` before writing, so the distance from there
				// to the file pointer is what reached the file. WriteFile does not
				// promise to set lpNumberOfBytesWritten on failure. If the pointer
				// cannot be read either, report the extent as unknown so the caller
				// retires the segment instead of treating it as untouched.
				LARGE_INTEGER zero, current;
				zero.QuadPart = 0;
				if (::SetFilePointerEx(this->fileHandle, zero, &current, FILE_CURRENT)) {
					bytesLanded = landedBytesFromFilePointer(
						current.QuadPart,
						static_cast<int64_t>(this->size.load(std::memory_order_relaxed)));
				} else {
					bytesLanded = TRANSACTION_LOG_BYTES_LANDED_UNKNOWN;
				}
				return -1;
			}

			if (bytesWritten == 0) {
				// shouldn't happen; bail to avoid an infinite loop
				DEBUG_LOG("%p TransactionLogFile::writeBatchToFile WriteFile returned 0 bytes (iovec %d/%d, %zu remaining)\n",
					this, i, iovcnt, remaining);
				bytesLanded = totalBytesWritten;
				return -1;
			}

			totalBytesWritten += bytesWritten;
			basePtr += bytesWritten;
			remaining -= bytesWritten;
		}
	}

	return totalBytesWritten;
}

int64_t TransactionLogFile::writeToFile(const void* buffer, uint32_t size, int64_t offset) {
	if (offset >= 0) {
		if (::SetFilePointer(this->fileHandle, offset, nullptr, FILE_BEGIN) == INVALID_SET_FILE_POINTER) {
			return -1;
		}
	} else {
		// offset < 0 means append to end of file
		if (::SetFilePointer(this->fileHandle, 0, nullptr, FILE_END) == INVALID_SET_FILE_POINTER) {
			return -1;
		}
	}

	DWORD bytesWritten;
	bool success = ::WriteFile(this->fileHandle, buffer, size, &bytesWritten, nullptr);
	return success ? static_cast<int64_t>(bytesWritten) : -1;
}

bool TransactionLogFile::truncateFile(uint32_t newSize) {
	if (this->fileHandle == INVALID_HANDLE_VALUE) {
		return false;
	}
	if (this->memoryMap && this->memoryMap.use_count() > 1) {
		DEBUG_LOG("%p TransactionLogFile::truncateFile Refusing to truncate %s with an outstanding memory map\n",
			this, this->path.string().c_str());
		return false;
	}

	this->memoryMap.reset();
	LARGE_INTEGER boundary;
	boundary.QuadPart = newSize;
	if (!::SetFilePointerEx(this->fileHandle, boundary, nullptr, FILE_BEGIN) ||
		!::SetEndOfFile(this->fileHandle)) {
		DEBUG_LOG("%p TransactionLogFile::truncateFile Failed to truncate %s to %u bytes (error=%lu)\n",
			this, this->path.string().c_str(), newSize, ::GetLastError());
		return false;
	}

	if (!::FlushFileBuffers(this->fileHandle)) {
		DWORD error = ::GetLastError();
		DEBUG_LOG("%p TransactionLogFile::truncateFile FlushFileBuffers failed for %s (error=%lu)\n",
			this, this->path.string().c_str(), error);
		throw rocksdb_js::DBException(
			"Failed to flush transaction log truncation: " + this->path.string());
	}
	return true;
}

bool TransactionLogFile::zeroTailLocked(uint32_t newSize) {
	uint32_t entriesEnd = this->size.load(std::memory_order_relaxed);
	if (this->fileHandle == INVALID_HANDLE_VALUE || entriesEnd <= newSize) {
		return false;
	}

	// Only the entries extent is rewritten: everything past `size` is already
	// the zero padding this file was pre-extended with, so a pre-extended
	// active segment costs one small write here, not a maxFileSize one.
	constexpr uint32_t chunkSize = 64 * 1024;
	std::vector<char> zeros(std::min(chunkSize, entriesEnd - newSize), 0);
	for (uint32_t offset = newSize; offset < entriesEnd; ) {
		uint32_t remaining = entriesEnd - offset;
		uint32_t toWrite = remaining < chunkSize ? remaining : chunkSize;
		int64_t written = this->writeToFile(zeros.data(), toWrite, static_cast<int64_t>(offset));
		if (written != static_cast<int64_t>(toWrite)) {
			DEBUG_LOG("%p TransactionLogFile::zeroTailLocked Failed to zero %s at offset %u (error=%lu)\n",
				this, this->path.string().c_str(), offset, ::GetLastError());
			return false;
		}
		offset += toWrite;
	}

	if (!::FlushFileBuffers(this->fileHandle)) {
		DEBUG_LOG("%p TransactionLogFile::zeroTailLocked FlushFileBuffers failed for %s (error=%lu)\n",
			this, this->path.string().c_str(), ::GetLastError());
		// The zeros are not durable yet, so a crash before the next flush could
		// still resurrect the torn bytes. Report failure rather than let the
		// caller record a boundary the file does not have.
		return false;
	}
	return true;
}

bool TransactionLogFile::eraseTail(uint32_t newSize, uint32_t entriesEnd) {
	if (this->fileHandle == INVALID_HANDLE_VALUE || entriesEnd <= newSize) {
		return false;
	}
	// These are whole entries a recovery decided to discard, so failing to
	// shrink the file is not an option to accept quietly: the bytes would be
	// swallowed into the next batch's group by that batch's last-entry flag.
	// Zeroing them is the equivalent erase when a live mapping blocks
	// SetEndOfFile (see zeroTailLocked).
	return this->truncateFile(newSize) || this->zeroTailLocked(newSize);
}

std::string getWindowsErrorMessage(DWORD errorCode) {
	if (errorCode == 0) {
		return "No error";
	}

	LPSTR messageBuffer = nullptr;
	size_t size = ::FormatMessageA(
		FORMAT_MESSAGE_ALLOCATE_BUFFER | FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS,
		nullptr,
		errorCode,
		MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT),
		(LPSTR)&messageBuffer,
		0,
		nullptr
	);

	std::string message(messageBuffer, size);
	::LocalFree(messageBuffer);

	// remove trailing newline characters
	while (!message.empty() && (message.back() == '\n' || message.back() == '\r')) {
		message.pop_back();
	}

	return message;
}

#if TRANSACTION_LOG_ENABLE_ANONYMOUS_OVERLAY
void TransactionLogFile::updateMemoryMapOverlay() {
	// No-op: Windows pre-extends the file to maxFileSize before mapping.
}
#endif

size_t TransactionLogFile::adviseCold() {
	// No-op: MADV_COLD is a Linux facility. Harper's memory-pressure target
	// (Linux containers / Fabric) does not include Windows.
	return 0;
}

} // namespace rocksdb_js

#endif

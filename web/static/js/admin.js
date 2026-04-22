document.addEventListener('DOMContentLoaded', function () {
    initAdmin();
});

let isDataUploaded = false;

function logInteraction(action, metadata = {}) {
    fetch('/api/log_interaction', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ action, metadata })
    }).catch(err => console.warn("Log interaction failed:", err));
}

function initAdmin() {
    loadAdminData();

    const etlBtn = document.getElementById('btn-trigger-etl');
    if (etlBtn) {
        etlBtn.addEventListener('click', triggerETL);
    }

    const uploadBtn = document.getElementById('btn-upload-trigger');
    const fileInput = document.getElementById('admin-file-input');

    if (uploadBtn && fileInput) {
        uploadBtn.addEventListener('click', () => fileInput.click());
        fileInput.addEventListener('change', (e) => {
            if (e.target.files.length > 0) {
                handleFileUpload(e.target.files[0]);
            }
        });
    }
}

function loadAdminData() {
    fetch('/api/admin/metrics')
        .then(res => {
            if (!res.ok) throw new Error("Metrics API failed");
            return res.json();
        })
        .then(data => {
            const mUsers = document.getElementById('metric-users');
            if (mUsers) mUsers.innerText = Number(data.total_users || 0).toLocaleString();

            const mSessions = document.getElementById('metric-sessions');
            if (mSessions) mSessions.innerText = Number(data.active_sessions || 0).toLocaleString();

            const mQueries = document.getElementById('metric-queries');
            if (mQueries) mQueries.innerText = data.queries_24h;

            const mSize = document.getElementById('metric-data-size');
            if (mSize) mSize.innerText = data.data_size;
        })
        .catch(err => console.error("Admin metrics failed:", err));

    fetch('/admin/users')
        .then(res => {
            if (!res.ok) throw new Error("Users API failed");
            return res.json();
        })
        .then(users => {
            const tbody = document.getElementById('table-users');
            if (!tbody) return;

            tbody.innerHTML = '';
            users.forEach(u => {
                const row = document.createElement('tr');
                row.className = 'bg-surface-container-low/40 hover:bg-surface-container-low transition-colors';
                row.innerHTML = `
                    <td class="px-4 py-4 rounded-l-xl">
                        <div class="flex items-center gap-3">
                            <div class="w-8 h-8 rounded-full bg-slate-200 flex items-center justify-center font-bold">${u.name[0]}</div>
                            <div>
                                <p class="text-sm font-bold">${u.name}</p>
                                <p class="text-[10px] text-on-surface-variant">${u.email}</p>
                            </div>
                        </div>
                    </td>
                    <td class="px-4 py-4 text-xs font-medium">${u.role}</td>
                    <td class="px-4 py-4 text-xs">${u.last_active}</td>
                    <td class="px-4 py-4 rounded-r-xl">
                        <div class="flex items-center justify-end gap-3">
                        <button data-user-id="${u.id}" data-user-name="${u.name}" class="delete-user text-[10px] font-bold uppercase tracking-wide text-red-600 hover:text-red-700">
                            Delete
                        </button>
                        <button data-user-id="${u.id}" class="status-toggle w-10 h-5 ${u.active ? 'bg-green-500' : 'bg-slate-300'} rounded-full relative transition-colors">
                            <div class="absolute inset-y-0.5 left-0.5 w-4 h-4 bg-white rounded-full shadow-sm"></div>
                        </button>
                        </div>
                    </td>
                `;
                tbody.appendChild(row);
            });

            tbody.querySelectorAll('.status-toggle').forEach(button => {
                button.addEventListener('click', () => {
                    const userId = button.dataset.userId;
                    const nextStatus = button.classList.contains('bg-green-500') ? 'disabled' : 'active';
                    updateUserStatus(userId, nextStatus, button);
                });
            });

            tbody.querySelectorAll('.delete-user').forEach(button => {
                button.addEventListener('click', () => {
                    const userId = button.dataset.userId;
                    const userName = button.dataset.userName;
                    deleteUser(userId, userName);
                });
            });
        })
        .catch(err => console.error("Admin users failed:", err));
}

function updateUserStatus(userId, status, button) {
    fetch(`/admin/users/${userId}/status`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ status })
    })
        .then(res => {
            if (!res.ok) throw new Error("Status update failed");
            return res.json();
        })
        .then(data => {
            const isActive = data.status === 'active';
            button.classList.toggle('bg-green-500', isActive);
            button.classList.toggle('bg-slate-300', !isActive);
            logInteraction('ADMIN_USER_STATUS_UPDATED', { user_id: userId, status: data.status });
        })
        .catch(err => {
            console.error("Status update failed:", err);
            alert('Unable to update user status right now.');
        });
}

function deleteUser(userId, userName) {
    const confirmed = window.confirm(`Soft delete ${userName}? This will hide the user from the admin list.`);
    if (!confirmed) return;

    fetch(`/admin/users/${userId}`, {
        method: 'DELETE'
    })
        .then(res => {
            if (!res.ok) throw new Error("Delete failed");
            return res.json();
        })
        .then(() => {
            logInteraction('ADMIN_USER_SOFT_DELETED', { user_id: userId, user_name: userName });
            loadAdminData();
        })
        .catch(err => {
            console.error("User delete failed:", err);
            alert('Unable to delete user right now.');
        });
}

function handleFileUpload(file) {
    const formData = new FormData();
    formData.append('file', file);

    fetch('/api/admin/upload-file', {
        method: 'POST',
        body: formData
    })
        .then(res => res.json())
        .then(data => {
            if (data.status === 'success') {
                alert('File uploaded successfully. You can now run the ETL pipeline.');
                enableETLButton();
                logInteraction('ADMIN_FILE_UPLOAD_SUCCESS', { filename: file.name });
            } else {
                alert('Upload failed: ' + data.message);
                logInteraction('ADMIN_FILE_UPLOAD_FAILED', { filename: file.name, error: data.message });
            }
        })
        .catch(err => {
            console.error("Upload error:", err);
            alert('Error uploading file. Check console.');
        });
}

function enableETLButton() {
    const etlBtn = document.getElementById('btn-trigger-etl');
    if (etlBtn) {
        etlBtn.classList.remove('opacity-50', 'cursor-not-allowed');
        etlBtn.classList.add('hover:opacity-90', 'ring-2', 'ring-secondary');
        isDataUploaded = true;
    }
}

function disableETLButton() {
    const etlBtn = document.getElementById('btn-trigger-etl');
    if (etlBtn) {
        etlBtn.classList.add('opacity-50', 'cursor-not-allowed');
        etlBtn.classList.remove('hover:opacity-90', 'ring-2', 'ring-secondary');
        isDataUploaded = false;
    }
}

function triggerETL() {
    if (!isDataUploaded) {
        alert('All is okay. (No new files uploaded for processing)');
        logInteraction('ADMIN_ETL_TRIGGER_SKIPPED', { reason: 'No new data' });
        return;
    }

    // Add confirmation
    const confirmRun = confirm("It will run automatically at 2 am with the uploaded data do you still want to continue.");
    if (!confirmRun) {
        logInteraction('ADMIN_ETL_TRIGGER_CANCELLED', { reason: 'User cancelled at confirmation' });
        return;
    }

    const overlay = document.getElementById('etl-overlay');
    if (overlay) overlay.classList.remove('hidden');

    fetch('/api/admin/upload', { method: 'POST' })
        .then(res => {
            if (!res.ok) throw new Error("ETL trigger failed at server side");
            return res.json();
        })
        .then(data => {
            if (overlay) overlay.classList.add('hidden');
            if (data.status === 'success') {
                alert('ETL Pipeline successfully re-ingested data!');
                disableETLButton();
                loadAdminData();
            } else if (data.status === 'ok') {
                alert(data.message);
                disableETLButton();
            } else {
                alert('ETL Pipeline failed: ' + data.error);
            }
        })
        .catch(err => {
            if (overlay) overlay.classList.add('hidden');
            console.error("ETL trigger failed:", err);
            alert('Error triggering execution. Check server console for details.');
        });
}

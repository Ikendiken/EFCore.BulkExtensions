using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;

namespace EFCore.BulkExtensions
{
    public static class DbContextBulkExtensions
    {
        public static void BulkInsert<T>(this DbContext context, IList<T> entities, BulkConfig bulkConfig = null, Action<decimal> progress = null) where T : class
        {
            DbContextBulkTransaction.Execute(context, entities, OperationType.Insert,  bulkConfig, progress);
        }

        public static void BulkInsertOrUpdate<T>(this DbContext context, IList<T> entities, BulkConfig bulkConfig = null, Action<decimal> progress = null) where T : class
        {
            DbContextBulkTransaction.Execute(context, entities, OperationType.InsertOrUpdate,  bulkConfig, progress);
        }

        public static void BulkUpdate<T>(this DbContext context, IList<T> entities, BulkConfig bulkConfig = null, Action<decimal> progress = null) where T : class
        {
            DbContextBulkTransaction.Execute(context, entities, OperationType.Update,  bulkConfig, progress);
        }

        public static void BulkDelete<T>(this DbContext context, IList<T> entities, BulkConfig bulkConfig = null, Action<decimal> progress = null) where T : class
        {
            DbContextBulkTransaction.Execute(context, entities, OperationType.Delete,  bulkConfig, progress);
        }

        // Async methods

        public static Task BulkInsertAsync<T>(this DbContext context, IList<T> entities, BulkConfig bulkConfig = null, Action<decimal> progress = null) where T : class
        {
            return DbContextBulkTransaction.ExecuteAsync(context, entities, OperationType.Insert,  bulkConfig, progress);
        }

        public static Task BulkInsertOrUpdateAsync<T>(this DbContext context, IList<T> entities, BulkConfig bulkConfig = null, Action<decimal> progress = null) where T : class
        {
            return DbContextBulkTransaction.ExecuteAsync(context, entities, OperationType.InsertOrUpdate,  bulkConfig, progress);
        }

        public static Task BulkUpdateAsync<T>(this DbContext context, IList<T> entities, BulkConfig bulkConfig = null, Action<decimal> progress = null) where T : class
        {
            return DbContextBulkTransaction.ExecuteAsync(context, entities, OperationType.Update,  bulkConfig, progress);
        }

        public static Task BulkDeleteAsync<T>(this DbContext context, IList<T> entities, BulkConfig bulkConfig = null, Action<decimal> progress = null) where T : class
        {
            return DbContextBulkTransaction.ExecuteAsync(context, entities, OperationType.Delete,  bulkConfig, progress);
        }

        // IQueryable support
        public static void BulkInsert<T>(this DbContext context, IQueryable<T> query, BulkConfig bulkConfig = null, Action<long, bool> progress = null) where T : class
        {
            DbContextBulkTransaction.Execute(context, query, OperationType.Insert, bulkConfig, progress);
        }

        public static void BulkInsertOrUpdate<T>(this DbContext context, IQueryable<T> query, BulkConfig bulkConfig = null, Action<long, bool> progress = null) where T : class
        {
            DbContextBulkTransaction.Execute(context, query, OperationType.InsertOrUpdate, bulkConfig, progress);
        }

        public static void BulkUpdate<T>(this DbContext context, IQueryable<T> query, BulkConfig bulkConfig = null, Action<long, bool> progress = null) where T : class
        {
            DbContextBulkTransaction.Execute(context, query, OperationType.Update, bulkConfig, progress);
        }

        public static void BulkDelete<T>(this DbContext context, IQueryable<T> query, BulkConfig bulkConfig = null, Action<long, bool> progress = null) where T : class
        {
            DbContextBulkTransaction.Execute(context, query, OperationType.Delete, bulkConfig, progress);
        }

        // IQueryable Async methods

        public static Task BulkInsertAsync<T>(this DbContext context, IQueryable<T> query, BulkConfig bulkConfig = null, Action<long, bool> progress = null) where T : class
        {
            return DbContextBulkTransaction.ExecuteAsync(context, query, OperationType.Insert, bulkConfig, progress);
        }

        public static Task BulkInsertOrUpdateAsync<T>(this DbContext context, IQueryable<T> query, BulkConfig bulkConfig = null, Action<long, bool> progress = null) where T : class
        {
            return DbContextBulkTransaction.ExecuteAsync(context, query, OperationType.InsertOrUpdate, bulkConfig, progress);
        }

        public static Task BulkUpdateAsync<T>(this DbContext context, IQueryable<T> query, BulkConfig bulkConfig = null, Action<long, bool> progress = null) where T : class
        {
            return DbContextBulkTransaction.ExecuteAsync(context, query, OperationType.Update, bulkConfig, progress);
        }

        public static Task BulkDeleteAsync<T>(this DbContext context, IQueryable<T> query, BulkConfig bulkConfig = null, Action<long, bool> progress = null) where T : class
        {
            return DbContextBulkTransaction.ExecuteAsync(context, query, OperationType.Delete, bulkConfig, progress);
        }
    }
}

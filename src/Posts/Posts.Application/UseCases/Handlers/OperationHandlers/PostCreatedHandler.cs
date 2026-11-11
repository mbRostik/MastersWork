using MassTransit;
using MediatR;
using Microsoft.EntityFrameworkCore;
using Posts.Application.Contracts.Interfaces;
using Posts.Application.Services;
using Posts.Application.UseCases.Commands;
using Posts.Application.UseCases.Notifications;
using Posts.Domain.Entities;
using Posts.Infrastructure.Data;
using System.Net.Http.Json;
using System.Security.Cryptography;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Posts.Application.UseCases.Handlers.Creation
{
    public interface IPostCreated_PublishEvent_From_PostWebApi
    {
        public Guid CorrelationId { get; }
        public PostPublishedDto Data { get; }
    }

    public class PostPublishedDto
    {
        public int PostId { get; set; }
        public string Title { get; set; }
        public string Commitment { get; set; }
        public int CommitmentVersion { get; set; }
        public string Content { get; set; }
        public string UserId { get; set; }
    }

    public class PostCreatedHandler : IRequestHandler<CreatePostCommand, bool>
    {
        public const string CategoryClient = "PostCategories";

        private sealed record CategoryPrediction([property: JsonPropertyName("predicted_categories")] string[]? Categories);

        private readonly IMediator mediator;
        private readonly PostDbContext dbContext;
        private readonly IMapperService mapperService;
        private readonly Serilog.ILogger logger;
        private readonly IPublishEndpoint publishEndpoint;
        private readonly IHttpClientFactory httpClientFactory;

        public PostCreatedHandler(PostDbContext dbContext, IMediator mediator, IMapperService mapperService,
            Serilog.ILogger logger, IPublishEndpoint publishEndpoint, IHttpClientFactory httpClientFactory)
        {
            this.dbContext = dbContext;
            this.mediator = mediator;
            this.mapperService = mapperService;
            this.logger = logger;
            this.publishEndpoint = publishEndpoint;
            this.httpClientFactory = httpClientFactory;
        }

        public async Task<bool> Handle(CreatePostCommand request, CancellationToken cancellationToken)
        {
            if (request.model.UserId == null || (request.model.Content == null && request.model.Title == null))
                return false;

            var mapper = mapperService.InitializeAutomapper_CreatePostDTO_To_Post();
            var post = mapper.Map<Post>(request.model);
            post.Title ??= string.Empty;
            post.Content ??= string.Empty;

            using var client = httpClientFactory.CreateClient(CategoryClient);
            using var textContent = JsonContent.Create(new { text = post.Title + ". " + post.Content });
            var textCategories = await PredictCategories(client, "/predict_category", textContent, cancellationToken);

            var photoCategories = Array.Empty<string>();
            if (post.Files?.Count > 0)
            {
                using var photos = new MultipartFormDataContent();
                foreach (var file in post.Files)
                {
                    var image = new ByteArrayContent(file.file);
                    image.Headers.ContentType = new System.Net.Http.Headers.MediaTypeHeaderValue("image/jpeg");
                    photos.Add(image, "files", file.Name);
                }
                photoCategories = await PredictCategories(client, "/predict_photo_category", photos, cancellationToken);
            }

            await using var transaction = await dbContext.Database.BeginTransactionAsync(cancellationToken);
            dbContext.Posts.Add(post);
            await dbContext.SaveChangesAsync(cancellationToken);

            post.CommitmentSalt = RandomNumberGenerator.GetBytes(32);
            post.Commitment = PostProofCommitment.Compute(post, post.CommitmentSalt);

            var textCategoryIds = new HashSet<int>();
            foreach (var name in textCategories)
            {
                var category = await dbContext.PostTextCategories
                    .FirstOrDefaultAsync(c => c.CategoryName == name, cancellationToken);
                if (category == null)
                {
                    category = new PostTextCategory { CategoryName = name };
                    dbContext.PostTextCategories.Add(category);
                    await dbContext.SaveChangesAsync(cancellationToken);
                }
                textCategoryIds.Add(category.Id);
            }
            foreach (var categoryId in textCategoryIds)
            {
                dbContext.PostsWithTextCategories.Add(new PostWithTextCategories
                {
                    PostId = post.Id,
                    PostTextCategoryId = categoryId
                });
            }

            var photoCategoryIds = new HashSet<int>();
            foreach (var name in photoCategories)
            {
                var category = await dbContext.PostPhotoCategories
                    .FirstOrDefaultAsync(c => c.CategoryName == name, cancellationToken);
                if (category == null)
                {
                    category = new PostPhotoCategory { CategoryName = name };
                    dbContext.PostPhotoCategories.Add(category);
                    await dbContext.SaveChangesAsync(cancellationToken);
                }
                photoCategoryIds.Add(category.Id);
            }
            foreach (var categoryId in photoCategoryIds)
            {
                dbContext.PostsWithPhotoCategories.Add(new PostWithPhotoCategories
                {
                    PostId = post.Id,
                    PostPhotoCategoryId = categoryId
                });
            }

            await publishEndpoint.Publish<IPostCreated_PublishEvent_From_PostWebApi>(new
            {
                CorrelationId = Guid.NewGuid(),
                Data = new PostPublishedDto
                {
                    PostId = post.Id,
                    Title = post.Title,
                    Content = post.Content,
                    UserId = post.UserId,
                    Commitment = post.Commitment,
                    CommitmentVersion = 1
                }
            }, cancellationToken);
            await mediator.Publish(new PostCreatedNotification(post), cancellationToken);

            await dbContext.SaveChangesAsync(cancellationToken);
            await transaction.CommitAsync(cancellationToken);
            logger.Information("Created PostId {PostId} and committed its outbox events", post.Id);
            return true;
        }

        private async Task<string[]> PredictCategories(HttpClient client, string path, HttpContent content,
            CancellationToken cancellationToken)
        {
            try
            {
                using var response = await client.PostAsync(path, content, cancellationToken);
                response.EnsureSuccessStatusCode();
                var prediction = await response.Content.ReadFromJsonAsync<CategoryPrediction>(cancellationToken);
                if (prediction?.Categories == null)
                {
                    logger.Warning("Category response from {Path} did not contain predicted_categories", path);
                    return [];
                }
                return prediction.Categories.Where(name => !string.IsNullOrWhiteSpace(name)).Distinct().ToArray();
            }
            catch (Exception ex) when (ex is HttpRequestException or JsonException
                || (ex is OperationCanceledException && !cancellationToken.IsCancellationRequested))
            {
                logger.Warning(ex, "Category prediction failed for {Path}", path);
                return [];
            }
        }
    }
}

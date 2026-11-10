using MassTransit;
using MediatR;
using Microsoft.EntityFrameworkCore;
using Posts.Application.Contracts.Interfaces;
using Posts.Application.Services;
using Posts.Application.UseCases.Commands;
using Posts.Application.UseCases.Notifications;
using Posts.Domain.Entities;
using Posts.Infrastructure.Data;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

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
        private readonly IMediator mediator;

        private readonly PostDbContext dbContext;
        private readonly IMapperService _mapper;
        private readonly Serilog.ILogger logger;
        private readonly IPublishEndpoint _publishEndpoint;

        public PostCreatedHandler(PostDbContext dbContext, IMediator mediator, IMapperService mapper, Serilog.ILogger logger, IPublishEndpoint publishEndpoint)
        {
            this.dbContext = dbContext;
            this.mediator = mediator;
            this._mapper = mapper;
            this.logger = logger;
            _publishEndpoint = publishEndpoint;
        }

        public async Task<bool> Handle(CreatePostCommand request, CancellationToken cancellationToken)
        {
            var mapper = _mapper.InitializeAutomapper_CreatePostDTO_To_Post();

            try
            {
                if (request.model.UserId == null || (request.model.Content == null && request.model.Title == null))
                {
                    return false;
                }
                Post temp = mapper.Map<Post>(request.model);
                temp.Title ??= string.Empty;
                temp.Content ??= string.Empty;
                await using (var transaction = await dbContext.Database.BeginTransactionAsync(cancellationToken))
                {
                    dbContext.Posts.Add(temp);
                    await dbContext.SaveChangesAsync(cancellationToken);

                    temp.CommitmentSalt = RandomNumberGenerator.GetBytes(32);
                    temp.Commitment = PostProofCommitment.Compute(temp, temp.CommitmentSalt);
                    await dbContext.SaveChangesAsync(cancellationToken);
                    await transaction.CommitAsync(cancellationToken);
                }

                await _publishEndpoint.Publish<IPostCreated_PublishEvent_From_PostWebApi>(new
                {
                    CorrelationId = Guid.NewGuid(),
                    Data = new PostPublishedDto
                    {
                        PostId = temp.Id,
                        Title = temp.Title,
                        Content = temp.Content,
                        UserId = temp.UserId,
                        Commitment = temp.Commitment,
                        CommitmentVersion = 1
                    }
                }, cancellationToken);

                var model = dbContext.Entry(temp);
                try
                {
                    using (var client = new HttpClient())
                    {
                        client.BaseAddress = new Uri("http://127.0.0.1:8000");

                        var contentData = new
                        {
                            text = request.model.Title + ". " + request.model.Content
                        };

                        var jsonContent = new StringContent(
                            JsonSerializer.Serialize(contentData),
                            Encoding.UTF8,
                            "application/json");

                        var response = await client.PostAsync("/predict_category", jsonContent);

                        if (response.IsSuccessStatusCode)
                        {
                            var responseContent = await response.Content.ReadAsStringAsync();

                            var jsonDoc = JsonDocument.Parse(responseContent);
                            var predictedCategories = jsonDoc.RootElement
                                .GetProperty("predicted_categories")
                                .EnumerateArray()
                                .Select(element => element.GetString())
                                .Where(x => !string.IsNullOrWhiteSpace(x))
                                .Distinct()
                                .ToList();

                            var categoryIds = new HashSet<int>();

                            foreach (var category in predictedCategories)
                            {
                                var existingCategory = await dbContext.PostTextCategories
                                    .FirstOrDefaultAsync(c => c.CategoryName == category);

                                if (existingCategory != null)
                                {
                                    categoryIds.Add(existingCategory.Id);
                                }
                                else
                                {
                                    var newCategory = new PostTextCategory
                                    {
                                        CategoryName = category
                                    };

                                    var tempCat = dbContext.PostTextCategories.Add(newCategory);
                                    await dbContext.SaveChangesAsync();
                                    categoryIds.Add(tempCat.Entity.Id);
                                }
                            }

                            foreach (var categoryId in categoryIds)
                            {
                                bool alreadyExists = dbContext.PostsWithTextCategories
                                    .Local.Any(x => x.PostId == model.Entity.Id && x.PostTextCategoryId == categoryId);

                                if (!alreadyExists)
                                {
                                    dbContext.PostsWithTextCategories.Add(new PostWithTextCategories
                                    {
                                        PostId = model.Entity.Id,
                                        PostTextCategoryId = categoryId
                                    });
                                }
                            }

                            await dbContext.SaveChangesAsync();

                            //try
                            //{
                            //    using var textGraphClient = new HttpClient();
                            //    textGraphClient.BaseAddress = new Uri("http://127.0.0.1:8002");

                            //    var textGraphPayload = new
                            //    {
                            //        user_id = model.Entity.UserId.ToString(),
                            //        post_id = model.Entity.Id.ToString(),
                            //        text_categories = categoryIds
                            //            .Select(id => id.ToString())
                            //            .ToList()
                            //    };

                            //    var textContent = new StringContent(
                            //        JsonSerializer.Serialize(textGraphPayload),
                            //        Encoding.UTF8,
                            //        "application/json");

                            //    await textGraphClient.PostAsync("/update-graph", textContent);
                            //}
                            //catch (Exception ex)
                            //{
                            //    logger.Warning(ex, "Failed to update TEXT graph for PostId {PostId}", model.Entity.Id);
                            //}
                        }
                        if (request.model.Files.Any())
                        {
                            using (var content = new MultipartFormDataContent())
                            {
                                foreach (var file in request.model.Files)
                                {
                                    var imageBytes = Convert.FromBase64String(file.Content);
                                    var imageContent = new ByteArrayContent(imageBytes);
                                    imageContent.Headers.ContentType = new System.Net.Http.Headers.MediaTypeHeaderValue("image/jpeg");

                                    // "files" must match FastAPI parameter name: List[UploadFile] = File(...)
                                    content.Add(imageContent, "files", file.Name);
                                }

                                var photoResponse = await client.PostAsync("/predict_photo_category", content);

                                if (photoResponse.IsSuccessStatusCode)
                                {
                                    var photoResponseContent = await photoResponse.Content.ReadAsStringAsync();

                                    var photoJsonDoc = JsonDocument.Parse(photoResponseContent);
                                    var photoCategories = photoJsonDoc.RootElement
                                        .GetProperty("predicted_categories")
                                        .EnumerateArray()
                                        .Select(element => element.GetString())
                                        .Distinct() // ✅ Prevent duplicates here
                                        .ToList();

                                    foreach (var photoCategory in photoCategories)
                                    {
                                        var existingPhotoCategory = await dbContext.PostPhotoCategories
                                            .FirstOrDefaultAsync(c => c.CategoryName == photoCategory);

                                        int photoCategoryId;
                                        if (existingPhotoCategory != null)
                                        {
                                            photoCategoryId = existingPhotoCategory.Id;
                                        }
                                        else
                                        {
                                            var newPhotoCategory = new PostPhotoCategory
                                            {
                                                CategoryName = photoCategory
                                            };
                                            dbContext.PostPhotoCategories.Add(newPhotoCategory);
                                            await dbContext.SaveChangesAsync();
                                            photoCategoryId = newPhotoCategory.Id;
                                        }

                                        // Check if this relation already exists in memory to avoid EF conflict
                                        bool alreadyTracked = dbContext.PostsWithPhotoCategories
                                            .Local.Any(x => x.PostId == model.Entity.Id && x.PostPhotoCategoryId == photoCategoryId);

                                        if (!alreadyTracked)
                                        {
                                            dbContext.PostsWithPhotoCategories.Add(new PostWithPhotoCategories
                                            {
                                                PostId = model.Entity.Id,
                                                PostPhotoCategoryId = photoCategoryId
                                            });
                                        }
                                    }

                                    await dbContext.SaveChangesAsync();

                                    //try
                                    //{
                                    //    using var photoGraphClient = new HttpClient();
                                    //    photoGraphClient.BaseAddress = new Uri("http://127.0.0.1:8003");

                                    //    var photoGraphPayload = new
                                    //    {
                                    //        user_id = model.Entity.UserId.ToString(),
                                    //        post_id = model.Entity.Id.ToString(),
                                    //        photo_categories = photoCategories
                                    //    };

                                    //    var photoContent = new StringContent(
                                    //        JsonSerializer.Serialize(photoGraphPayload),
                                    //        Encoding.UTF8,
                                    //        "application/json");

                                    //    await photoGraphClient.PostAsync("/update-photo-graph", photoContent);
                                    //}
                                    //catch (Exception ex)
                                    //{
                                    //    logger.Warning(ex, "Failed to update PHOTO graph for PostId {PostId}", model.Entity.Id);
                                    //}
                                }
                            }
                        }
                       
                        logger.Information("Post with ID {PostId} created successfully", model.Entity.Id);

                    }
                }
                catch (HttpRequestException ex)
                {
                    logger.Warning(ex, "Category service unavailable for PostId {PostId}", temp.Id);
                }

                await mediator.Publish(new PostCreatedNotification(temp), cancellationToken);
                return true;

            }
            catch (Exception ex)
            {
                logger.Error(ex, "Error creating post. {ErrorMessage}", ex.Message);
                return false;
            }

        }

    }
}

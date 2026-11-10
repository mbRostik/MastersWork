using System.Globalization;
using System.Numerics;
using System.Security.Cryptography;
using System.Text;
using Nethereum.Util;
using Posts.Domain.Entities;

namespace Posts.Application.Services;

public static class PostProofCommitment
{
    public static string AnalysisText(Post post) => string.Join(". ",
        new[] { post.Title, post.Content }.Where(value => !string.IsNullOrWhiteSpace(value)));

    public static string Compute(Post post, byte[] salt)
    {
        if (salt.Length != 32)
            throw new ArgumentException("Expected a 32-byte post salt.", nameof(salt));

        var hasher = new PoseidonHasher();
        var subject = hasher.Hash(Digest(post.UserId), post.Id, Digest(AnalysisText(post)));
        var high = new BigInteger(salt.AsSpan(0, 16), isUnsigned: true, isBigEndian: true);
        var low = new BigInteger(salt.AsSpan(16, 16), isUnsigned: true, isBigEndian: true);
        return hasher.Hash(subject, high, low).ToString(CultureInfo.InvariantCulture);
    }

    private static BigInteger Digest(string value) => new(
        SHA256.HashData(Encoding.UTF8.GetBytes(value)).AsSpan(0, 31),
        isUnsigned: true, isBigEndian: true);
}
